var fs = require('fs');

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
