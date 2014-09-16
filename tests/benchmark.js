(function(){
    // search radii to use
    var radii = [1000, 5000, 10000, 50000, 100000, 500000, 1000000, 2000000];

    // default number of runs
    var runs = radii.length;
    
    // default path to the leveldb database
    var path = '/tmp/levelgeo.db';

    // process the arguments
    for(var i1=2;i1<process.argv.length;i1++){
	if("--path" == process.argv[i1] && i1+1 < process.argv.length){
            ++i1;
            path = process.argv[i1];
	}else if("--runs" == process.argv[i1] && i1+1 < process.argv.length){
            ++i1;
            runs = parseInt(process.argv[i1]);
	}else if("--help" == process.argv[i1]){
	    console.log("Usage: node benchmark.js --path <path to database> --runs <number of runs>");
	    return;
	}
    }
    
    if(runs > radii.length){
	console.log("The specified number of runs ("+runs+") is greater than the maximum allowable ("+radii.length+").");
	return;
    }
    
    // ensure that the selected path is not empty
    var util = require('util');
    var fs = require('fs');
    var exists = fs.existsSync(path);
    if(!exists){
	console.log("The specified location ("+path+") does not exist or is empty: please specify a valid lcoation.");
	return;
    }
    
    // open the database
    console.log("Using database location: "+path+".");
	
    var pad = function(value, size)
    {
	var res = ""+value;
	while(res.length < size) res = " "+res;
	return res;
    };

    // open the database
    var levelup = require('levelup');
    var dboptions = {
        keyEncoding: 'utf8',
        valueEncoding: 'json',
        compression: false,
        cacheSize: 64*1024*1024,
        createIfMissing: false,
        disableCompact: true,
	db: require('leveldown')
    };
    levelup(path, dboptions, function(err, ndb){
	if(err){
	    console.log("An error occurred opening the database: "+err);
	    return;
	}
	
	// get the top level settings
	var settings = null;
	ndb.get('config', function(err, val){
	    if(err || val == null){
		console.log("Invalid database: no configuration entry found.");
		return;
	    }
	    settings = JSON.parse(val);
	    console.log("Database configuration: "+util.inspect(settings));

	    // create the geo database
	    geo = require('../lib/level-geospatial')(ndb);
	    
	    console.log("Geo database initialized.");
	    
	    // pick the center point
	    var lat = (settings.topleft.lat-settings.lowerright.lat)/2 + settings.lowerright.lat;
	    var lon = (settings.topleft.lon-settings.lowerright.lon)/2 + settings.lowerright.lon;
	    console.log("Using center point "+lat +","+lon);
	    
	    var run_search = function(radius_index)
	    {
		var result_count = 0;
		var start = Date.now();
		geo.search({ lat: lat, lon: lon }, radii[radius_index]).on('data', function(data){
                    ++result_count;
		}).on('end', function(){
		    var end = Date.now();
		    var interval = (end - start)/1000;
		    console.log("Search radius: "+pad(radii[radius_index], 10)+"m\tResults: "+pad(result_count, 10)+"\tDuration: "+pad(interval.toFixed(4), 8)+" seconds.");
		    
		    var next_index = radius_index + 1;
		    if(next_index >= runs || next_index >= radii.length){
			return;
		    }else{
			setImmediate(run_search, next_index);
		    }
		});
	    };

	    // and start
	    run_search(0);
	});
    });
})();
