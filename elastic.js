/* jshint esversion:6 */

var _ = require('lodash');
var log4js = require('log4js');
var fs = require('graceful-fs');
var path = require('path');
var async = require('async');
var MailParser = require('mailparser').MailParser;
var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

var insertCounter = 0;

const MAIL_DIR = '/Users/wyngc1/Downloads/OrionMailArchive';
// const MAIL_DIR = '/Users/wyngc1/Downloads/OrionMailArchive/Archived!62';
const INDEX = 'email';
const ASYNC_LIMIT = 1000;

var logger = log4js.getLogger();
logger.setLevel('INFO');

var processingStarted = new Date();

var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err){
      return done(err);
    }
    var pending = list.length;
    if (!pending) {
      return done(null, results);
    }
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


client.indices.delete({index:INDEX}, (error, response) =>{
  if(error){
    logger.warn(error);
  }

  client.indices.create(
    {
    'index':'email',
    body:{'settings':{'analysis':{'analyzer':{'urls-links-emails':{'type':'custom','tokenizer':'uax_url_email'}}}},
    'mappings':
    {'emails':{
      'properties':{
        'mailto':{'type':'string','analyzer':'urls-links-emails'},
        'mailfrom':{'type':'string','analyzer':'urls-links-emails'},
        'cc':{'type':'string','analyzer':'urls-links-emails'},
        'bcc':{'type':'string','analyzer':'urls-links-emails'},
        'body':{'type':'string','analyzer':'english'}
      }
    }}}
    },
    (error, response) => {
      if(error){
        logger.error(error);
      }
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
            mailparser.on('end', (mail_object) => {
              logger.debug(
                'Date:' + mail_object.date + ' Subject:' + mail_object.subject +
                ' From:' + JSON.stringify(mail_object.from) +
                ' To:' + JSON.stringify(mail_object.to) +
                ' CC:' + JSON.stringify(mail_object.cc) +
                ' BCC:' + JSON.stringify(mail_object.bcc) +
                ' Message-ID:' + JSON.stringify(mail_object.headers['message-id'])
              );

              var cleanArray = (field) => {
                var result = [];
                if(field && field.length > 0){
                  result = _.map(
                    field,
                    function(item){
                      return _.toLower(item.address);
                    }
                  );
                }
                return result;
              };

              var fromAddress = cleanArray(mail_object.from);
              var toAddress = cleanArray(mail_object.to);
              var ccAddress = cleanArray(mail_object.cc);
              var bccAddress = cleanArray(mail_object.bcc);

              // index the mail
              var docToInsert = {
                messageid:mail_object.headers['message-id'],
                datesent:mail_object.date,
                mailfrom:fromAddress,
                mailto:toAddress,
                cc:ccAddress,
                bcc:bccAddress,
                subject:_.trim(mail_object.subject),
                body:mail_object.text
              };

              client.index({
                index: INDEX,
                type: 'email',
                id: mail_object.headers['message-id'],
                body: docToInsert
              }, function (error, response) {
                if(error){
                  logger.error('Failed', error);
                  callback(error);
                }
                else{
                  logger.debug(response);
                  callback();
                }
              });

            }
          );
          fs.createReadStream(file).pipe(mailparser);
        });


      }
      });
    }
  );


});
