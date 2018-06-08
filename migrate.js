/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2018 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

/*
 We allow this script to crash at any time.
 Only S3 errors are handled, and failed items are logged in log/failed.txt.
 If crash happens when multiple uploads are in progress, none of the files will
 be accepted to S3 in any case, because AWS SDK takes care of file integrity checking.
 
 For each uploaded item a key in Redis is set. Redis host must have enough memory,
 to hold all keys.
 */

const fs = require("fs");
const elasticsearch = require('elasticsearch');
const ReadableSearch = require('elasticsearch-streams').ReadableSearch;
const through2Concurrent = require('through2-concurrent');
const AWS = require('aws-sdk');
const redis = require("redis");
const config = require('./config');

const esClient = new elasticsearch.Client({
	host: config.es.host
});

const s3Client = new AWS.S3(config.s3);

const redisClient = redis.createClient({
	host: config.redis.host,
	port: config.redis.port
});

const uploadedPath = 'log/uploaded.txt';
const failedPath = 'log/failed.txt';

let finished = false;

let nPerSecond = 0;
let nUploaded = 0;
let nFailed = 0;
let nSkipped = 0;
let nActive = 0;

let scrollId = null;

let esScrollStream = new ReadableSearch(function (from, callback) {
	if (scrollId) {
		esClient.scroll({
			scrollId: scrollId,
			scroll: '1h'
		}, function (err, resp) {
			if (err) throw err;
			callback(null, resp);
		});
	}
	else {
		esClient.search({
			index: config.es.index,
			scroll: '1h',
			// Should be enough for any count of concurrent s3 uploads
			size: 50,
			body: {
				query: {match_all: {}}
			}
		}, function (err, resp) {
			if (err) throw err;
			scrollId = resp._scroll_id;
			callback(err, resp);
		});
	}
});

let s3UploadStream = through2Concurrent.obj(
	{maxConcurrency: config.concurrentUploads, highWaterMark: 16},
	function (item, enc, callback) {
		
		// Check if the item isn't already updated in S3
		redisClient.get('s3:' + item._id, function (err, res) {
			if (err) throw err;
			
			if (res) {
				nSkipped++;
				return callback();
			}
			
			// Check if the whole library isn't already updated (deleted) in S3
			redisClient.get('s3:' + item._id.split('/')[0], function (err, res) {
				if (err) throw err;
				
				if (res) {
					nSkipped++;
					return callback();
				}
				
				let parts = item._id.split('/');
				
				// ES indexed fulltexts don't have 'key', but we want it in S3
				item._source.key = parts[1];
				
				nActive++;
				let params = {Key: item._id, Body: JSON.stringify(item._source)};
				s3Client.upload(params, function (err) {
					if (err) {
						fs.appendFileSync(failedPath, item._id + '\n');
						nFailed++;
					}
					else {
						fs.appendFileSync(uploadedPath, item._id + '\n');
						nUploaded++;
						redisClient.set('s3:' + item._id, '1');
					}
					
					nActive--;
					
					nPerSecond++;
					
					callback();
				});
			})
		});
	});

esScrollStream.pipe(s3UploadStream);

s3UploadStream.on('finish', function () {
	finished = true;
});

setInterval(function () {
	console.log(nPerSecond + '/s, active: ' + nActive + ',  uploaded: ' + nUploaded + ', skipped: ' + nSkipped + ', failed: ' + nFailed);
	nPerSecond = 0;
	if (finished && nActive === 0) {
		console.log('done');
		process.exit();
	}
}, 1000);
