const path = require('path');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');
const readline = require('readline');

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
let modifyStartTime = 0;
let totalModifiedFiles = 0;
let totalFailedFiles = 0;
let filesNotFound = [];
let statusIntervalTimeout;
let startDate = new Date();
let outputData = {
    startTime: startDate.toUTCString(),
    filesToModify: filesToModify,
    filesNotFound: filesNotFound,
    summary: {},
};

function printProgress(progress){
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(progress);
}

function printModifyProgress() {
    printProgress(`Status: Modified files: ${totalModifiedFiles} (${(totalModifiedFiles / filesToModify.length * 100).toFixed(2)}%); Failed: ${totalFailedFiles}; Elapsed time: ${(new Date().getTime() - modifyStartTime) / 1000} Seconds`);
}

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

    let files = inputData.files.concat(inputData.directories);

    files.forEach((file) => {
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

    modifyStartTime = new Date().getTime();
    statusIntervalTimeout = setInterval(printModifyProgress, 1000);
    async.eachLimit(filesToModify, argv.parallelFiles, async.reflect(modifyFileTimestamps), callback);
}

function createOutputFile() {
    let outputStream = fs.createWriteStream(argv.outputFile);
    outputStream.write(JSON.stringify(outputData));
    outputStream.end();
}

modifyFiles((err) => {
    if (statusIntervalTimeout) {
        clearInterval(statusIntervalTimeout);
    }
    printModifyProgress();

    if (err) {
        console.error(`\r\nModification completed with error: ${err}`);
    } else {
        console.info(`\r\nModification completed. ${argv.dryRun ? '(Dry run)' : ''}`);
    }

    outputData.summary.duration = (new Date().getTime() - startTime) / 1000;
    outputData.summary.modifyDuration = argv.dryRun ? 0 : outputData.summary.duration - ((modifyStartTime - startTime) / 1000);

    console.info('Summary:');
    console.info(`\tDuration: ${outputData.summary.duration} seconds`);
    console.info(`\tModification duration: ${outputData.summary.modifyDuration} seconds`);
    console.info(`\tModified files: ${totalModifiedFiles}`);
    console.info(`\tFailed files: ${totalFailedFiles}`);
    console.info(`\tFiles not found: ${filesNotFound.length}`);

    outputData.summary = {
        totalModifiedFiles: totalModifiedFiles,
        totalFailedFiles: totalFailedFiles,
        totalFilesNotFound: filesNotFound.length,
    };

    createOutputFile();
});
