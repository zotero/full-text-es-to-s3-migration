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
 
 Dataserver sets a Redis key for newly uploaded fulltext too.
 
 After crash ES have to be scanned again, but since all the uploaded fulltext ids
 are in Redis, we can easily skip those items.
 */

const fs = require("fs");
const elasticsearch = require('elasticsearch');
const ReadableSearch = require('elasticsearch-streams').ReadableSearch;
const through2Concurrent = require('through2-concurrent');
const AWS = require('aws-sdk');
const redis = require('redis');
const RedisClustr = require('redis-clustr');
const config = require('./config');
const zlib = require('zlib');

const esClient = new elasticsearch.Client({
	host: config.es.host,
	apiVersion: '0.90'
});

const s3Client = new AWS.S3(config.s3);

const redisClient = new RedisClustr({
	servers: [{host: config.redis.host, port: config.redis.port}],
	createClient: function (port, host, options) {
		return redis.createClient(port, host, options);
	},
	redisOptions: {
		prefix: config.redis.prefix
	}
});

const uploadedPath = 'log/uploaded.txt';
const failedPath = 'log/failed.txt';
const scrollPath = 'log/scroll.txt';

let finished = false;

let nPerSecond = 0;
let nUploaded = 0;
let nFailed = 0;
let nSkipped = 0;
let nActive = 0;

let shuttingDown = false;
let waitingForESResponse = false;

let scrollId = null;

let fetchedIds = [];

(async function main() {
	await reprocess();
	
	if (fs.existsSync(scrollPath)) {
		scrollId = fs.readFileSync(scrollPath).toString();
		fs.unlinkSync(scrollPath);
		console.log('Continuing from ' + scrollId);
	}
	
	stream();
})();

async function reprocess() {
	if (!fs.existsSync(failedPath)) return;
	fetchedIds = fs.readFileSync(failedPath).toString().split('\n');
	
	let n = 0;
	while (fetchedIds.length) {
		let id = fetchedIds[0];
		console.log(`reprocessing ${id}, left: ${fetchedIds.length}`);
		let item = await esClient.get({
			index: config.es.index,
			type: config.es.type,
			id: id,
			routing: id.split('/')[0]
		});
		
		await new Promise(function (resolve, reject) {
			processItem(item, function (err) {
				if (err) return reject(err); // Critical error
				resolve();
			})
		});
	}
	
	fs.unlinkSync(failedPath);
}

function processItem(item, callback) {
	// Check if the item isn't already updated in S3
	redisClient.get('s3:' + item._id, function (err, res) {
		if (err) return callback(err); // Critical error
		
		if (res) {
			nSkipped++;
			fetchedIds.splice(fetchedIds.indexOf(item._id), 1);
			return callback();
		}
		
		// Check if the whole library isn't already updated (deleted) in S3
		redisClient.get('s3:' + item._id.split('/')[0], function (err, res) {
			if (err) return callback(err); // Critical error
			
			if (res) {
				nSkipped++;
				fetchedIds.splice(fetchedIds.indexOf(item._id), 1);
				return callback();
			}
			
			let parts = item._id.split('/');
			
			// ES indexed fulltexts don't have 'key', but we want it in S3
			item._source.key = parts[1];
			
			let json = item._source;
			json = JSON.stringify(json);
			
			// 'json' now becomes a buffer
			json = zlib.gzipSync(json);
			
			nActive++;
			let params = {
				Key: item._id,
				Body: json,
				ContentType: 'application/gzip',
				StorageClass: json.length < config.minFileSizeStandardIA ? 'STANDARD' : 'STANDARD_IA'
			};
			s3Client.upload(params, function (err) {
				if (err) {
					nFailed++;
				}
				else {
					fs.appendFileSync(uploadedPath, item._id + '\n');
					nUploaded++;
					redisClient.set('s3:' + item._id, '2');
				}
				
				fetchedIds.splice(fetchedIds.indexOf(item._id), 1);
				
				nActive--;
				
				nPerSecond++;
				
				callback();
			});
		})
	});
}

function stream() {
	let esScrollStream = new ReadableSearch(function (from, callback) {
		if (shuttingDown) return; // Stop fetching items if shutdown is in progress
		if (scrollId) {
			waitingForESResponse = true;
			esClient.scroll({
				scrollId: scrollId,
				scroll: '48h'
			}, function (err, resp) {
				waitingForESResponse = false;
				if (err) throw err; // Critical error
				resp.hits.hits.forEach(x => fetchedIds.push(x._id));
				callback(null, resp);
			});
		}
		else {
			waitingForESResponse = true;
			esClient.search({
				index: config.es.index,
				type: config.es.type,
				scroll: '48h',
				size: 50,
				body: {
					query: {match_all: {}}
				}
			}, function (err, resp) {
				waitingForESResponse = false;
				if (err) throw err; // Critical error
				resp.hits.hits.forEach(x => fetchedIds.push(x._id));
				scrollId = resp._scroll_id;
				callback(err, resp);
			});
		}
	});
	
	let s3UploadStream = through2Concurrent.obj(
		{maxConcurrency: config.concurrentUploads, highWaterMark: 16},
		function (item, enc, callback) {
			processItem(item, function (err) {
				if (err) throw err; // Critical error
				callback();
			});
		});
	
	esScrollStream.pipe(s3UploadStream);
	
	s3UploadStream.on('finish', function () {
		finished = true;
	});
	
	let interval = setInterval(function () {
		console.log(nPerSecond + '/s, active: ' + nActive + ',  uploaded: ' + nUploaded + ', skipped: ' + nSkipped + ', failed: ' + nFailed);
		nPerSecond = 0;
		if (finished && nActive === 0) {
			console.log('Finished processing all items');
			clearInterval(interval);
			shutdown();
		}
	}, 1000);
}

process.on('SIGTERM', function () {
	console.log("Received SIGTERM");
	shutdown();
});

process.on('SIGINT', function () {
	console.log("Received SIGINT");
	shutdown();
});

process.on('uncaughtException', function (err) {
	console.log("Uncaught exception:", err);
	shutdown();
});

process.on("unhandledRejection", function (reason, promise) {
	console.log('Unhandled Rejection at:', promise, 'reason:', reason);
	shutdown();
});

function shutdown() {
	console.log('Shutting down');
	
	shuttingDown = true;
	
	// Make sure there is no in-flight queries for ES
	if (waitingForESResponse) {
		console.log('Waiting for ES response');
		return setTimeout(shutdown, 1000);
	}
	
	if (fetchedIds.length) {
		fs.writeFileSync(failedPath, fetchedIds.join('\n'));
	}
	
	if (scrollId) {
		fs.writeFileSync(scrollPath, scrollId);
	}
	
	process.exit();
}
