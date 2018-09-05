const path = require('path');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');

const argv = require('yargs')
    .usage('Usage: $0 -i inputFilePath -p targetPath -o outputFile [--parallelFiles number] [-t]')
    .example('-i ./out-upload.json -p /temp/s3 -o ./output/out-mod.json --parallelFiles 200 -t')
    .describe('i', 'Input file path')
    .alias('i', 'inputFile')
    .describe('p', 'Target folder path')
    .alias('p', 'trgPath')
    .describe('o', 'Output log file path')
    .alias('o', 'outputFile')
    .describe('parallelFiles', 'Parallel files')
    .default('parallelFiles', 200)
    .describe('t', 'Test without changing timestamps')
    .boolean('t')
    .alias('t', 'dryRun')
    .demandOption(['i', 'p', 'o'])
    .argv;

const inputData = require(argv.inputFile);
const trgPath = path.resolve(argv.trgPath);
const filesToModify = [];
let startTime = 0;
let totalModifiedFiles = 0;
let totalFailedFiles = 0;
let filesNotFound = [];
let outputData = {
    filesToModify: filesToModify,
    filesNotFound: filesNotFound,
};

function modifyFileTimestamps(file, callback) {
    try {
        fs.utimesSync(file.path, file.atime, file.mtime);
        totalModifiedFiles++;
        file.status = 1;
        callback();
    } catch (err) {
        console.error(`Failed to modify ${file.path}`);
        totalFailedFiles++;
        callback(err);
    }
}

function modifyFiles(callback) {
    startTime = new Date().getTime();

    inputData.files.forEach((file) => {
        let trgFile = path.join(trgPath, file.relPath);
        if (fs.existsSync(trgFile)) {
            filesToModify.push({
                path: trgFile,
                atime: file.atime/1000,
                mtime: file.mtime/1000,
                status: 0,
            })
        } else {
            filesNotFound.push(trgFile);
        }
    });

    console.log(`Modifying ${trgPath}:\r\n\tFiles: ${filesToModify.length}\r\n\tFiles not found: ${filesNotFound.length}`);
    if (argv.dryRun) {
        return callback();
    }

    async.eachLimit(filesToModify, argv.parallelFiles, async.reflect(modifyFileTimestamps), callback);
}

function createOutputFile() {
    let outputStream = fs.createWriteStream(argv.outputFile);
    outputStream.write(JSON.stringify(outputData));
    outputStream.end();
}

modifyFiles((err) => {
    if (err) {
        console.error(`\r\nModify completed with error: ${err}`);
    } else {
        console.info(`\r\nUploaded completed. ${argv.dryRun ? '(Dry run)' : ''}`);
    }

    let duration = (new Date().getTime() - startTime) / 1000;
    console.info('Summary:');
    console.info(`\tDuration: ${duration} seconds`);
    console.info(`\tModified files: ${totalModifiedFiles}`);
    console.info(`\tFailed files: ${totalFailedFiles}`);
    console.info(`\tFiles not found: ${filesNotFound.length}`);

    outputData.summary = {
        duration: duration,
        totalModifiedFiles: totalModifiedFiles,
        totalFailedFiles: totalFailedFiles,
        totalFilesNotFound: filesNotFound.length,
    };

    createOutputFile();
});
