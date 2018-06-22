module.exports = {
	es: {
		host: '',
		index: '',
	},
	s3: {
		params: {
			Bucket: ''
		},
		accessKeyId: '',
		secretAccessKey: ''
	},
	concurrentUploads: 2,
	redis: {
		host: '',
		port: ''
	},
	minFileSizeStandardIA: 75 * 1024
};