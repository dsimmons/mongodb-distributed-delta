// Global requires
cluster = require('cluster');
mongo = require('mongoskin'),
db = mongo.db('localhost:27017/research');

if (cluster.isMaster) {
	require(__dirname+'/master.js');
} else {
	require(__dirname+'/worker.js');
}
