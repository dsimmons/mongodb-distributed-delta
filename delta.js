// Global requires
cluster = require('cluster');
db = require('mongoskin').db('localhost:27017/research');

if (cluster.isMaster) {
	require(__dirname+'/lib/master.js');
} else {
	require(__dirname+'/lib/worker.js');
}
