var fs = require("fs");
var elasticsearch = require('elasticsearch');
var ReadableSearch = require('elasticsearch-streams').ReadableSearch;
var through2Concurrent = require('through2-concurrent');
var AWS = require('aws-sdk');
var redis = require("redis");
var config = require('./config');

var esClient = new elasticsearch.Client({
	host: config.esHost
});

var s3Client = new AWS.S3({
	accessKeyId: config.s3AccessKeyId,
	secretAccessKey: config.s3SecretAccessKey
});

var redisClient = redis.createClient({host: config.redisHost, port: config.redisPort});

// To prevent Redis unhandled exceptions
redisClient.on("error", function (err) {
	console.log("Redis error " + err);
});

var nSecond = 0;
var nSuccessful = 0;
var nFailed = 0;
var nSkipped = 0;
var nActive = 0;

var fileSuccessful = fs.createWriteStream('log/successful.txt');
var fileFailed = fs.createWriteStream('log/failed.txt');

var scroll_id = config.scrollId;

var esScrollStream = new ReadableSearch(function (from, callback) {
	if (scroll_id) {
		esClient.scroll({
			scrollId: scroll_id,
			scroll: '24h'
		}, function (err, resp) {
			if (err) return failure(err);
			callback(null, resp);
		});
	} else {
		esClient.search({
			index: config.esIndex,
			// If this script would fail,
			// we would have time to continue from the already existing scroll id
			scroll: '24h',
			// Should be enough for any count of concurrent s3 uploads
			size: 50,
			body: {
				query: {match_all: {}}
			}
		}, function (err, resp) {
			if (err) return failure(err);
			scroll_id = resp._scroll_id;
			fs.appendFileSync('./log/scroll.txt', scroll_id + '\n');
			callback(err, resp);
		});
	}
});

var s3UploadStream = through2Concurrent.obj(
	{maxConcurrency: config.s3Concurrent, highWaterMark: 16},
	function (chunk, enc, callback) {
		redisClient.get('s3:' + chunk._id, function (err, res) {
			// This shouldn't happen if redis connection has error handler.
			// Redis connection errors should be recoverable.
			if (err) {
				fileFailed.write(chunk._id + '\n');
				nFailed++;
				return failure();
			}
			
			if (res) {
				nSkipped++;
				return callback();
			}
			
			var source = {};
			if (chunk._source) {
				source = chunk._source;
			}
			
			var parts = chunk._id.split('/');
			source.key = parts[1];
			
			nActive++;
			var params = {Bucket: config.s3Bucket, Key: chunk._id, Body: JSON.stringify(source)};
			s3Client.upload(params, function (err, data) {
				if (err) {
					fileFailed.write(chunk._id + '\n');
					nFailed++;
				} else {
					fileSuccessful.write(chunk._id + '\n');
					nSuccessful++;
				}
				
				nActive--;
				nSecond++;
				callback();
			});
		});
	});

esScrollStream.pipe(s3UploadStream);

esScrollStream.on('end', function () {
	console.log('Done, but please wait until all active uploads will be finished');
});

setInterval(function () {
	console.log(nSecond + '/s, active: ' + nActive + ',  successful: ' + nSuccessful + ', skipped: ' + nSkipped + ', failed: ' + nFailed);
	nSecond = 0;
}, 1000);

function failure(err) {
	console.log('Unrecoverable failure. Wait for all uploads to finish before killing', err);
}
