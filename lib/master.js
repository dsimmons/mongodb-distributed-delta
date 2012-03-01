// File-specific (local) imports
var exec = require('child_process').exec;
var os = require('os');
var path = require('path');

// File-specific (local) variables
var child_processes = os.cpus().length;
var directory = process.cwd();
var workers = [];
var filePaths = [];
var count = readyCount = 0;
var fullMode = verbose = false;

cli(); 		// Parse command-line arguments
init(); 	// Spawn workers, set up event handlers
start();	// Find CSVs, split work load evenly and pass to workers


///////////////////////////////////////////////
/* Process command-line arguments 			 */
/* Everything serves a purpose, don't touch. */
///////////////////////////////////////////////
function cli() {
	for (var i = 2; process.argv[i]; i++) {
		switch (process.argv[i]) {
			case '-c':
			case '--children':
				if (process.argv[++i]) {
					child_processes = process.argv[i];
				}
				break;

			case '--full':
				fullMode = true;
				console.log('Full database update enabled.');
				break;

			case '-v':
			case '--verbose':
				verbose = true;
				console.log('Verbose output enabled.');
				break;

			case '-d':
			case '--directory':
				if (process.argv[++i] && !path.existsSync(process.argv[i]))
					console.log('Specified directory does not exist!');
				else {
					directory = process.argv[i];
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
}

///////////////////////////////////////////////
/* Spawn workers, set up event handlers		 */
///////////////////////////////////////////////
function init() {
	if (verbose) { 
		console.log('Root directory set to ' + directory);
		console.log('Child processes set to ' + child_processes);
	}
	console.log('Spawning ' + child_processes +' workers...');

	for (var i = 0; i < child_processes; i++) {
		var worker = cluster.fork();
		if (verbose) console.log('Worker ' + worker.pid + ' online!');

		worker.on('message', function(msg) {
			// Master receieved message from a worker, examine message contents.
			if (msg.ready) {
				if (++readyCount === workers.length) {
					if (verbose) console.log("All workers indicated they're ready, sending start signal...");
					workers.forEach(function(worker) {
						worker.send( { start: true, fullMode: fullMode, verbose: verbose } );
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
					// Sometimes we have to wait briefly for the master process to catch up.
					setInterval(function() {
						if (count === filePaths.length) { 
							console.log('Finished successfully!');
							workers.forEach(function(worker) {
								if (verbose) console.log('Killing worker ' + worker.pid);
								worker.kill();
							});
							process.exit(0); 
						} else { console.log('Waiting for async calls to finish... ('+count+')'); }
					}, 1000);
				}
			}
		});

		workers.push(worker);
	}
}

/////////////////////////////////////////////////////////////////
/* Find file paths, split up work evenly and send to workers.  */
/////////////////////////////////////////////////////////////////
function start() {
	if (verbose) console.log(workers.length + ' workers spawned successfully! Starting execution...');
	// Default max buffer was too small.
	exec("find " + directory  + " -type f -name '*.csv'", {maxBuffer: 5000*1024}, function(err, stdout, stderr) {
		if (err) { console.log('ERR: ' + err); }
		if (stderr) { console.log('STDERR: ' + stderr); }

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
}
