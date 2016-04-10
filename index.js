var cassandra = require('cassandra-driver');
var _ = require('lodash');
var log4js = require('log4js');
var fs = require('graceful-fs');
var path = require('path');
var async = require('async');
var MailParser = require('mailparser').MailParser;

var insertCounter = 0;

// const MAIL_DIR = '/Users/wyngc1/Downloads/OrionMailArchive/Archived!1';
const MAIL_DIR = '/Users/wyngc1/Downloads/OrionMailArchive';
const ASYNC_LIMIT = 1000;
const CQL_INSERT_MAIL = 'INSERT INTO mail (messageid, datesent, yearsent, monthsent, dayofmonthsent, mailfrom, mailto, subject) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';

// configure logger
log4js.configure(
  {
    appenders: [
      { type: 'console'}
    ]
  }
);
var logger = log4js.getLogger();
logger.setLevel('INFO');




var client = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'myemail'});

client.on('log', function(level, message) {
  logger.debug('log event: %s -- %j', level, message);
});


var processingStarted = new Date();

var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var pending = list.length;
    if (!pending) return done(null, results);
    list.forEach(function(file) {
      file = path.resolve(dir, file);
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          walk(file, function(err, res) {
            results = results.concat(res);
            if (!--pending) done(null, results);
          });
        } else {
          results.push(file);
          // do work
          logger.debug('Visiting ' + file);

          if (!--pending) done(null, results);
        }
      });
    });
  });
};

walk(MAIL_DIR, function(err, results){
  if(err){
    logger.error(err);
  }
  else{
    logger.info('Directories traversed (' + results.length +' files visited)');

    // only interested in eml format files
    var emlFiles = _.filter(results, function(file){return file.endsWith('.eml');});
    logger.info(emlFiles.length + ' eml files');

    async.forEachOfLimit(emlFiles, ASYNC_LIMIT, function(file, index, callback){
      var mailparser = new MailParser();
      mailparser.on('end', function(mail_object){
        logger.debug(
          'Date:' + mail_object.date
          + ' Subject:' + mail_object.subject
          + ' From:' + JSON.stringify(mail_object.from)
          + ' To:' + JSON.stringify(mail_object.to)
          + ' CC:' + JSON.stringify(mail_object.cc)
          + ' BCC:' + JSON.stringify(mail_object.bcc)
          + ' Message-ID:' + JSON.stringify(mail_object.headers['message-id'])
        );

        var fromAddress;
        if(mail_object.from && mail_object.from.length > 0){
          fromAddress = mail_object.from[0].address;
        }
        var toAddress;
        if(mail_object.to && mail_object.to.length > 0){
          toAddress = mail_object.to[0].address;
        }

        client.execute(CQL_INSERT_MAIL,
          [
            mail_object.headers['message-id'],
            mail_object.date,
            mail_object.date.getFullYear(),
            mail_object.date.getMonth(),
            mail_object.date.getDate(),
            fromAddress,
            toAddress,
            mail_object.subject
          ],
          {prepare: true},
          function(err, result) {
            if (err){
              logger.error(err);
              callback(err);
            }
            else{
              process.stdout.write(".");
              insertCounter++;
              callback();
            }
          }
        );

      }
    );
    fs.createReadStream(file).pipe(mailparser);
  });


}
});
