var cluster = require('cluster');
var os = require('os');
var exec = require('child_process').exec;
var path = require('path');
var fs = require('fs');

var mongo = require('mongoskin'),
	db = mongo.db('localhost:27017/research');

if (cluster.isMaster) {
	//var child_processes = (process.argv[2]) ? process.argv[2] : os.cpus().length;
	var child_processes = 15; // hard-coded for now, TODO: parse based on arg flags
	var workers = [];
	var filePaths = [];
	var directory = process.argv[2]; // TODO: parse based on arg flags
	var count = readyCount = 0;

	console.log('Spawning ' + child_processes +' workers...');
	for (var i = 0; i < child_processes; i++) {
		var worker = cluster.fork();
		console.log('Worker ' + worker.pid + ' online!');
		worker.on('message', function(msg) {
			if (msg.ready) {
				if (++readyCount === workers.length) {
					console.log("All workers indicated they're ready, sending start signal...");
					workers.forEach(function(worker) {
						worker.send( { start: true } );
					});
				}
			} else if (msg.data) {
				//console.log(msg.data.ticker || msg.data, msg.data.date || '');
				count++;
				// TODO: write back to DB

			} else if (msg.done) {
				console.log('Worker ' + msg.done + ' signaled that it is finished.');
				if (--readyCount === 0) {
					console.log('All workers finished!');
					setInterval(function() {
						if (count === filePaths.length) { 
							console.log('Finished successfully!');
							workers.forEach(function(worker) {
								worker.kill();
							});
							process.exit(0); 
						}	
						else {
							console.log('Waiting for async calls to finish... ('+count+')');
						}
					}, 2000);
				}
			}
		});
		workers.push(worker);
	}
	console.log(workers.length + ' workers spawned successfully! Starting execution...');

	var dir = (process.argv[2] && path.existsSync(process.argv[2])) ? process.argv[2] : process.cwd();

	exec("find " + dir + " -type f -name '*.csv'", {maxBuffer: 5000*1024}, function(err, stdout, stderr) {
		if (err) { console.log(err); }
		if (stderr) { console.log(stderr); }

		filePaths = stdout.split('\n');
		filePaths.pop(); // contains a blank element at the end
		console.log('Discovered ' + filePaths.length + ' CSVs.');
		console.log('Beginning to diff...');
		
		var chunkSize = Math.floor(filePaths.length / workers.length); // no integer division in JS
		var begin = 0, end = chunkSize + (filePaths.length % workers.length); // handle non-even work load
		workers.forEach(function(worker) {
			worker.send( { queue: filePaths.slice(begin, end) } );
			begin = end; end = end+chunkSize; // begin inclusive, end exclusive
		});
	});

} else {
	// TODO: something going on with $ symbol, not in the database
	var fileList = null;
	process.on('message', function(msg) {
		if (msg.queue) {
			fileList = msg.queue;
			console.log('Worker ' + process.pid + ' recieved ' + fileList.length + ' CSVs to process.');
			process.send( { ready: true } );
		} else if (msg.start) {
			console.log('Worker ' + process.pid + ' recieved start signal.');
			fileList.forEach(function(file) {
				var lines = fs.readFileSync(file).toString().split('\n');
				lines.pop();
				var delta = [];
				var last = lines[lines.length-1].split(',');
				var date = last[1];
			
				db.collection('ohlc')
				.find({'ticker': last[0]})
				.sort({'date': -1}).limit(1)
				.toArray(function(err, query) {
					if (err) { console.log(err); }
					if (query && query.length) {
						var document = query.pop();
						if (document.ticker && document.date) {
							//console.log(document.ticker + ' last updated: ' + document.date);

							// Figure out difference between last DB document & latest CSV date
							// Then skip to that line in the file, verify, and insert everything after
							var csv_date = [], db_date = [];
							var d = document.date;
							//TODO: some off by one error, too tired to figure out
							db_date.push(
								d.getFullYear(),
								(d.getMonth() > 9) ? d.getMonth() : '0'+(d.getMonth()), // 0 - 11
								(d.getDate() > 9) ? d.getDate() : '0'+d.getDate());
							csv_date.push(date.substr(0,4), date.substr(4,2), date.substr(6,2));
							//console.log('DB: ', db_date);
							//console.log('CSV: ', csv_date);
							var delta_year = csv_date[0] - db_date[0];
							var delta_month = csv_date[1] - db_date[1];
							var delta_day = csv_date[2] - db_date[2];
							//console.log('Diff ' + document.ticker + ' | years: ' + delta_year +
										//' months: ' + delta_month + ' days: ' + delta_day);
							process.send( { data: document } );
						} else { process.send ( { data: 'notworking: ' + last[0] } ); }
					} else { process.send ( { data: 'notworking: ' + last[0] } ); }
				});
			});
			process.send( { done: process.pid } );
		}
	});
}
