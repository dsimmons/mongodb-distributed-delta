// File-specific (local) imports
var fs = require('fs');
var ohlc = db.collection('ohlc'); ohlc.emitter.setMaxListeners(0); 
var calculations = require(__dirname+'/calculations.js');

// File-specific (local) variables
var fileList = null;
var fullMode = verbose = false;

// Recieved message from master process, decide what to do.
process.on('message', function(msg) {
	// Recieved list of files from master to process.
	// ACK back 'ready'
	if (msg.queue) {
		fileList = msg.queue;
		if (verbose) console.log('Worker ' + process.pid + ' recieved ' + fileList.length + ' CSVs to process.');
		process.send( { ready: true } );

	// Receieved start signal from master, begin work we've been given.
	} else if (msg.start) {
		if (msg.fullMode) fullMode = msg.fullMode; 	// delta or full
		if (msg.verbose) verbose = msg.verbose;		// verbose output to stdout

		if (verbose) console.log('Worker ' + process.pid + ' recieved start signal.');
		fileList.forEach(function(file) {
			var lines = fs.readFileSync(file).toString().split('\n');
			lines.pop();
			var delta = [];
			var last = lines[lines.length-1].split(',');
			var date = last[1];

			ohlc
			.find({'ticker': last[0]})
			.sort({'date': -1}).limit(1)
			.toArray(function(err, query) {
				if (err) { console.log(err); }
				if (query && query.length) {
					//console.log('ohlc emitter: ' + ohlc.emitter);
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
							
							// Below two cases are temporary, handle errors for the time being.
					} else { process.send ( { data: 'notworking: ' + last[0] } ); }
				} else { process.send ( { data: 'notworking: ' + last[0] } ); }
			});
		});

		// Finished all of the work that we have.
		// FIN with 'who we are'
		process.send( { done: process.pid } );
	}
});
