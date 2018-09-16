const path = require('path');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');
const readline = require('readline');

const argv = require('yargs')
    .usage('Usage: $0 --accessKeyId accessKeyId --secretAccessKey secretAccessKey --region region -p srcPath -b bucketName -o outputFile [-d testPath] [--partSize number] [--queueSize number] [--parallelFiles number] [-i filePath] [-x filePath] [-t]')
    .example('--accessKeyId 123 --secretAccessKey 456 --region us-east-1 -p /temp/s3 -b bucketName -o ./output/out-upload.json -d testPath --partSize 10485760 --queueSize 5 --parallelFiles 20 -i includeFoldersPath -x excludeFoldersPath -t')
    .describe('accessKeyId', 'AWS access key ID')
    .describe('secretAccessKey', 'AWS secret access key')
    .describe('region', 'AWS region')
    .describe('p', 'Path of folder or file')
    .alias('p', 'srcPath')
    .describe('b', 'S3 bucket')
    .alias('b', 'bucketName')
    .describe('d', 'Path in bucket')
    .alias('d', 'dstPath')
    .default('d', '')
    .describe('o', 'Output log file path')
    .alias('o', 'outputFile')
    .describe('partSize', 'S3 upload part size (bytes)')
    .default('partSize', 10 * 1024 * 1024)
    .describe('queueSize', 'S3 upload queue size')
    .default('queueSize', 5)
    .describe('parallelFiles', 'Parallel files')
    .default('parallelFiles', 20)
    .describe('i', 'Include folders file path')
    .describe('x', 'Exclude folders file path')
    .describe('t', 'Test without uploading to S3')
    .boolean('t')
    .alias('t', 'dryRun')
    .demandOption(['accessKeyId', 'secretAccessKey', 'region', 'p', 'b', 'o'])
    .argv;

const srcPath = path.resolve(argv.srcPath);
const AWS = require('aws-sdk');
const S3 = new AWS.S3({
    accessKeyId: argv.accessKeyId,
    secretAccessKey: argv.secretAccessKey,
    region: argv.region
});
const filesToUpload = [];
const includeFolders = [];
const excludeFolders = [];
const actualExcludedFolders = [];
const foldersToUploadMetadata = [];
let startTime = 0;
let totalEmptyFoldersToUpload = 0;
let totalFilesToUpload = 0;
let totalSize = 0;
let totalUploadedFiles = 0;
let totalUploadedFilesSize = 0;
let totalUploadedEmptyFolders = 0;
let totalFailedFiles = 0;
let totalFailedFilesSize = 0;
let totalFailedEmptyFolders = 0;
let statusIntervalTimeout;
let outputStreamTemp;
let outputData = {
    srcPath: srcPath,
    files: [],
};

function createOutputFileTemp() {
    outputStreamTemp = fs.createWriteStream(argv.outputFile + '.tmp');
    outputStreamTemp.write('{');
    outputStreamTemp.write(`"srcPath": "${srcPath}",\n`);
    outputStreamTemp.write('"files": [\n');
}

function logger(data) {
    if (!outputStreamTemp) {
        return;
    }
    outputStreamTemp.write(`${JSON.stringify(data)},\n`);
}

function closeOutputFileTemp(summary) {
    if (!outputStreamTemp) {
        return;
    }
    outputStreamTemp.write('],\n');
    outputStreamTemp.write(`"summary":${JSON.stringify(summary)}`);
    outputStreamTemp.write('}');
    outputStreamTemp.end();
}

function createOutputFile() {
    let outputStream = fs.createWriteStream(argv.outputFile);
    outputStream.write(JSON.stringify(outputData));
    outputStream.end();
}

function printProgress(progress){
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(progress);
}

function getFoldersList(filePath, destArr, callback) {
    let includeFoldersPath = path.resolve(filePath);

    if (!fs.existsSync(includeFoldersPath)) {
        console.error(`Can't find ${includeFoldersPath} file!`);
        process.exit(-1);
    }
    let stat = fs.statSync(includeFoldersPath);
    if (!stat.isFile()) {
        console.error(`${includeFoldersPath} is not a file!`);
        process.exit(-1);
    }

    const rl = readline.createInterface({
        input: fs.createReadStream(includeFoldersPath),
        crlfDelay: Infinity
    });

    rl.on('line', (line) => {
        destArr.push(line);
    });

    rl.on('close', callback);
}

function walkSync(currentDirPath) {
    let dirName = path.basename(currentDirPath);
    if (_.includes(excludeFolders, dirName)) {
        console.log(`Excluding ${currentDirPath} directory - in exclude list`);
        actualExcludedFolders.push(currentDirPath);
        return;
    }
    if (includeFolders.length && dirName.startsWith('t_') && !_.includes(includeFolders, dirName)) {
        console.log(`Excluding ${currentDirPath} directory - not in tenants include list`);
        return;
    }

    let fileNames = fs.readdirSync(currentDirPath);
    if (fileNames.length === 0) {
        let stat = fs.statSync(currentDirPath);
        filesToUpload.push({
            filePath: currentDirPath,
            bucketPath: path.join(argv.dstPath, path.join(path.basename(srcPath), path.join(currentDirPath.substring(srcPath.length + 1), '.keep'))),
            stat: stat,
            uploadStatus: 0,
            isEmpty: true,
        });
        totalEmptyFoldersToUpload++;
        return;
    }
    fileNames.forEach((name) => {
        let filePath = path.join(currentDirPath, name);
        let stat = fs.statSync(filePath);
        if (stat.isFile()) {
            filesToUpload.push({
                filePath: filePath,
                bucketPath: path.join(argv.dstPath, path.join(path.basename(srcPath), filePath.substring(srcPath.length + 1))),
                stat: stat,
                uploadStatus: 0,
                isEmpty: false,
            });
            totalFilesToUpload++;
            totalSize += stat.size;
        } else if (stat.isDirectory()) {
            foldersToUploadMetadata.push({
                folderPath: filePath,
                stat: stat,
            });
            walkSync(filePath);
        }
    });
}

function uploadFile(item, callback) {
    let upload = new AWS.S3.ManagedUpload({
        partSize: argv.partSize,
        queueSize: argv.queueSize,
        params: {
            Bucket: argv.bucketName,
            Key: item.bucketPath,
            Body: item.isEmpty ? '' : fs.createReadStream(item.filePath)
        },
        tags: [
            { Key: 'atime', Value: item.stat.atime.getTime() },
            { Key: 'mtime', Value: item.stat.mtime.getTime() },
            { Key: 'ctime', Value: item.stat.ctime.getTime() },
            { Key: 'birthtime', Value: item.stat.birthtime.getTime() }
        ],
        service: S3
    });

    upload.send((err, data) => {
        if (err) {
            console.error(`Failed to upload file: ${item.filePath}. Error: ${err}`);
            if (item.isEmpty) {
                totalFailedEmptyFolders++;
            } else {
                totalFailedFiles++;
                totalFailedFilesSize += item.stat.size;
            }
            item.uploadStatus = -1;
        } else {
            if (item.isEmpty) {
                totalUploadedEmptyFolders++;
            }
            else {
                totalUploadedFiles++;
                totalUploadedFilesSize += item.stat.size;
            }
            item.uploadStatus = 1;
        }

        let outputLine = {
            relPath: path.relative(srcPath, item.filePath),
            bucketPath: item.bucketPath,
            atime: item.stat.atime.getTime(),
            mtime: item.stat.mtime.getTime(),
            ctime: item.stat.ctime.getTime(),
            birthtime: item.stat.birthtime.getTime(),
            status: item.uploadStatus,
        };
        logger(outputLine);
        outputData.files.push(outputLine);

        callback(err);
    });
}

function printUploadProgress() {
    printProgress(`Status: Uploaded files: ${totalUploadedFiles} (${(totalUploadedFiles / totalFilesToUpload * 100).toFixed(2)}%); Size: ${totalUploadedFilesSize} bytes (${(totalUploadedFilesSize / totalSize * 100).toFixed(2)}%); Failed: ${totalFailedFiles}`);
}

function uploadDir(dir, callback) {
    async.parallel([
            (callback) => argv.i ? getFoldersList(argv.i, includeFolders, callback) : callback(),
            (callback) => argv.x ? getFoldersList(argv.x, excludeFolders, callback) : callback()
        ],
        (err) => {
            if (err) {
                return callback(err);
            }

            walkSync(dir);
            console.log(`Uploading ${path.join(argv.bucketName, argv.dstPath)}:\r\n\tFiles: ${totalFilesToUpload}\r\n\tTotal size: ${totalSize}\r\n\tTotal empty folders: ${totalEmptyFoldersToUpload}`);
            if (argv.dryRun) {
                return callback();
            }
            statusIntervalTimeout = setInterval(printUploadProgress, 1000);
            async.eachLimit(filesToUpload, argv.parallelFiles, async.reflect(uploadFile), callback);
        }
    );
}

function upload(callback) {
    startTime = new Date().getTime();
    let stat = fs.statSync(srcPath);
    if (stat.isDirectory()) {       // upload directory recursively
        uploadDir(srcPath, callback);
    } else if (stat.isFile()) {    // upload single file
        totalSize += stat.size;
        console.log(`Uploading ${srcPath} to ${path.join(argv.bucketName, argv.dstPath)}. Total size: ${totalSize}`);
        if (argv.dryRun) {
            callback();
            return;
        }
        uploadFile(
            {
                filePath: srcPath,
                bucketPath: path.join(argv.dstPath, path.win32.basename(srcPath)),
                stat: stat,
                uploadStatus: 0
            },
            callback);
    }
}

function logFoldersMetadata() {
    let outputLine;

    foldersToUploadMetadata.forEach((folder) => {
        outputLine = {
            relPath: path.relative(srcPath, folder.folderPath),
            atime: folder.stat.atime.getTime(),
            mtime: folder.stat.mtime.getTime(),
            ctime: folder.stat.ctime.getTime(),
            birthtime: folder.stat.birthtime.getTime(),
            directory: 1,
        };
        logger(outputLine);
        outputData.files.push(outputLine);
    });
}

createOutputFileTemp();

upload((err) => {
    if (statusIntervalTimeout) {
        clearInterval(statusIntervalTimeout);
    }
    printUploadProgress();

    let summary = {
        duration: (new Date().getTime() - startTime) / 1000,
    };

    if (err) {
        console.error(`\r\nUpload completed with error: ${err}`);
        summary.status = `Upload completed with error: ${err}`;
    } else {
        console.info(`\r\nUploaded completed. ${argv.dryRun ? '(Dry run)' : ''}`);
        summary.status = `Uploaded completed. ${argv.dryRun ? '(Dry run)' : ''}`;
    }

    logFoldersMetadata();

    console.info('Summary:');
    console.info(`\tDuration: ${summary.duration} seconds`);
    console.info(`\tTotal uploaded files: ${totalUploadedFiles}`);
    console.info(`\tTotal uploaded files size: ${totalUploadedFilesSize}`);
    console.info(`\tTotal uploaded empty folders: ${totalUploadedEmptyFolders}`);
    console.info(`\tExcluded Folders: ${actualExcludedFolders.join(`${path.delimiter} `)}`);
    console.info(`\tFailed files: ${totalFailedFiles}`);
    console.info(`\tFailed empty folders: ${totalFailedEmptyFolders}`);

    summary.totalUploadedFiles = totalUploadedFiles;
    summary.totalUploadedFilesSize = totalUploadedFilesSize;
    summary.totalUploadedEmptyFolders = totalUploadedEmptyFolders;
    summary.actualExcludedFolders = actualExcludedFolders;
    summary.totalFailedFiles = totalFailedFiles;
    summary.totalFailedEmptyFolders = totalFailedEmptyFolders;

    closeOutputFileTemp(summary);

    outputData.summary = summary;

    createOutputFile();
});
