'use strict';

const fs = require('fs'),
  os = require('os'),
  path = require('path'),
  exec = require('child_process').exec,
  AWS = require('aws-sdk'),
  AdmZip = require('adm-zip'),
  MongoDBURI = require('mongodb-uri');

// Validate Configuration
function ValidateConfig(config) {
  if (config && config.mongodb && config.s3 && config.s3.accessKey && config.s3.secretKey && config.s3.region && config.s3.bucketName) {

    // Don't try to parse the url when it has been parsed once
    if (typeof config.mongodb == 'string') {
      config.mongodb = MongoDBURI.parse(config.mongodb)
    }

    return true;
  }
  return false;
}

// Set up AWS
function AWSSetup(config) {
  AWS
    .config
    .update({accessKeyId: config.s3.accessKey, secretAccessKey: config.s3.secretKey, region: config.s3.region});

  let s3 = new AWS.S3();
  return s3;
}

// Fetch a list of all files from AWS
function listObjectsInBucket(s3, config) {
  return new Promise((resolve, reject) => {
    const opts = {
      Bucket: config.s3.bucketName
    }
    s3.listObjects(opts, (err, data) => {
      if (err) {
        reject(err)
      } else {
        resolve(data.Contents)
      }
    });
  });
}

// Restore downloaded database
function restore(config, filePath) {

  return new Promise((resolve, reject) => {

    const database = config.mongodb.database,
      password = config.mongodb.password || null,
      username = config.mongodb.username || null,
      host = config.mongodb.hosts[0].host || null,
      port = config.mongodb.hosts[0].port || null;

    let gzipFlag = "--gzip"
    let dropFlag = ""

    if (path.extname(filePath).toLowerCase() != ".gz") {
      gzipFlag = ""
    }

    if (config.drop) {
      dropFlag = ' --drop'
    }

    const parts = filePath.split(/[\/\\]+/) //Split by slashes
    const file = parts[parts.length - 1]
    const originalDb = file.split('_')[0]

    console.log('originalDb', originalDb)

    const separator = filePath.indexOf('/') > -1 ? '/' : '\\'

    filePath += separator + originalDb
    console.log('filePath', filePath);

    // Default command, does not considers username or password
    let command = `mongorestore -h ${host} --port=${port} -d ${database} ${gzipFlag} ${dropFlag} ${filePath}`;

    // When Username and password is provided
    if (username && password) {
      command = `mongorestore -h ${host} --port=${port} -d ${database} -p ${password} -u ${username} ${gzipFlag} ${dropFlag} ${filePath}`;
    }
    // When Username is provided
    if (username && !password) {
      command = `mongorestore -h ${host} --port=${port} -d ${database} -u ${username} ${gzipFlag} ${dropFlag} ${filePath}`;
    }

    console.log('command', command)

    exec(command, {maxBuffer: 1024 * 500}, (err, stdout, stderr) => {
      if (err) {
        // Most likely, mongodump isn't installed or isn't accessible
        reject({error: 1, code: err.code, message: err.message});
      } else {
        resolve({
          error: 0,
          message: "Successfuly Restored Backup",
          backupName: path.basename(filePath)
        });
      }
    });
  });

}

function downloadAndUnzipBackup (s3, config, backupKey) {
  console.log(`downloading then unzip`)
  return new Promise((resolve, reject) => {
    console.log(`downloading`)
    return downloadBackup(s3, config, backupKey)
      .then(result => {
        console.log('dl result', result)
        const zip = new AdmZip(result.filePath)
        const extractTo = result.filePath.replace('.zip', '')
        console.log('extractTo', extractTo)
        zip.extractAllTo(extractTo)
        result.filePath = extractTo
        console.log('126result', result)
        resolve(result)
      })
  })
}

// Download Backup
function downloadBackup(s3, config, backupKey) {
  console.log('backupKey', backupKey);

  const obj = {
    Bucket: config.s3.bucketName,
    Key: backupKey
  }

  console.log('obj', obj)

  return new Promise((resolve, reject) => {
    console.log(`downloading`)
    const s3 = new AWS.S3()
    const stream = fs.createWriteStream(path.resolve(os.tmpdir(), backupKey))

    stream.on('error', err => {
      console.log('err', err)
      reject({error: 1, message: err.message, code: err.code})
      return
    })

    s3.getObject(obj)
      .on('httpData', function(chunk) {
        stream.write(chunk)
      })
      .on('httpDone', function() {
        stream.end()
        console.log(`finish called`)
        resolve({
          error: 0,
          filePath: path.resolve(os.tmpdir(), backupKey)
        })
      })
      .send()
    })
  })
}

// Validate, Download, Restore Backup
function RestoreBackup(config, databaseToRestore) {
  console.log('databaseToRestore', databaseToRestore);
  console.log('config', config);
  let isValidConfig = ValidateConfig(config)

  if (isValidConfig) {
    let s3 = AWSSetup(config)
    return downloadAndUnzipBackup(s3, config, databaseToRestore).then(result => {
      console.log('result', result)
      return restore(config, result.filePath)
    }, error => {
      return Promise.reject({error: 1, message: error.message});
    })

  } else {
    console.log(`not a valid confirguration`)
    return Promise.reject({error: 1, message: "Invalid Configuration"})
  }
}

// List Backups
function List(config) {
  let isValidConfig = ValidateConfig(config);

  if (isValidConfig) {
    let s3 = AWSSetup(config);
    return listObjectsInBucket(s3, config)
  } else {
    return Promise.reject({error: 1, message: "Invalid Configuration"})
  }
}

module.exports = {
  List: List,
  Restore: RestoreBackup
}