module.exports = {
	esHost: 'http://localhost:9200',
	esIndex: 'item_fulltext_index_read',
	s3Bucket: 'bucket-name',
	s3AccessKeyId: '',
	s3SecretAccessKey: '',
	s3Concurrent: 200,
	redisHost: 'localhost',
	redisPort: '6379',
	scrollId: '' // If entered, scroll will continue
};