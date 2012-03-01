var exec = require('child_process').exec;
var os = require('os');
var path = require('path');

var child_processes = os.cpus().length;
var directory = process.cwd();
var workers = [];
var filePaths = [];
var count = readyCount = 0;
var doFull = verbose = false;

// Handling of command line args, don't touch (everything has a purpose).
for (var i = 2; process.argv[i]; i++) {
	switch (process.argv[i]) {

		case '-c':
		case '--children':
			if (process.argv[++i]) {
				child_processes = process.argv[i];
				console.log('Child processes set to ' + process.argv[i]);
			}
			break;

		case '--full':
			doFull = true;
			console.log('Full database update enabled.');
			break;

		case '-v':
		case '--verbose':
			verbose = true;
			console.log('Verbose output enabled.');
			break;

		case '-d':
		case '--directory':
			if (process.argv[++i] && !path.existsSync(process.argv[i])) {
				console.log('Specified directory does not exist!');
			} else {
				directory = process.argv[i];
				console.log('Root directory set to ' + process.argv[i]);
				break;
			}

		case '-h':
		case '--help':
		default:
		// Trial and error to get correct tab alignment (maybe add %10c <-- formatting later)
		console.log('\nUsage: node delta [options]');
		console.log('\t-c, --children \t\tNumber of child processes.\t\tdefault: # cores');
		console.log('\t-d, --directory \tRoot directory to search for CSVs.\tdefault: cwd');
		console.log('\t--full \t\t\tFull update and consistency check\tdefault: diff');
		console.log('\t-v, --verbose \t\tVerbose printing to stdout.\t\tdefault: disabled');
		process.exit(1);
	}
}

console.log('Spawning ' + child_processes +' workers...');
for (var i = 0; i < child_processes; i++) {
	var worker = cluster.fork();
	if (verbose) console.log('Worker ' + worker.pid + ' online!');

	worker.on('message', function(msg) {
		if (msg.ready) {
			if (++readyCount === workers.length) {
				if (verbose) console.log("All workers indicated they're ready, sending start signal...");
				workers.forEach(function(worker) {
					worker.send( { start: true } );
				});
			}
		} else if (msg.data) {
			console.log(msg.data.ticker || msg.data, msg.data.date || '');
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
				}, 1000);
			}
		}
	});
	workers.push(worker);
}
if (verbose) console.log(workers.length + ' workers spawned successfully! Starting execution...');

exec("find " + directory  + " -type f -name '*.csv'", {maxBuffer: 5000*1024}, function(err, stdout, stderr) {
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

