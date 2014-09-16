(function(){
    // position data from http://en.wikipedia.org/wiki/Extreme_points_of_the_United_States
    // upper left location: 49°23′04.1″N 95°9′12.2″W  => 49.384472, -95.153389
    var topleft = [49.384472, -95.153389];
    // lower right location: 24°31′15″N 81°57′49″W => 24.520833, -81.963611
    var lowerright = [24.520833, -81.963611];
    
    // default spacing (in meters)
    var spacing = 2075;
    
    // default density
    var density = 1;
    
    // default path to the leveldb database
    var path = '/tmp/levelgeo.db';
    
    // whether or not we should overwrite
    var overwrite = false;
    
    // process the arguments
    for(var i1=2;i1<process.argv.length;i1++){
	if("--path" == process.argv[i1] && i1+1 < process.argv.length){
            ++i1;
            path = process.argv[i1];
	}else if("--spacing" == process.argv[i1] && i1+1 < process.argv.length){
            ++i1;
            spacing = parseInt(process.argv[i1]);
	}else if("--density" == process.argv[i1] && i1+1 < process.argv.length){
            ++i1;
            density = parseInt(process.argv[i1]);
	}else if("--overwrite" == process.argv[i1]){
	    overwrite = true;
	}else if("--help" == process.argv[i1]){
	    console.log("Usage: node createdb.js --path <path to database> --overwrite --spacing <space between points in meters> --density <number of database entries per point>");
	    return;
	}
    }
    
    // ensure that the selected path is empty
    var fs = require('fs');
    var uuid = require('uuid');

    // increase the longitude by <spacing> meters to the right: if we exceed the lowerright's right boundary,
    // increase the latitude by <spacing> meters and use topleft's longitude
    // see http://en.wikipedia.org/wiki/Decimal_degrees for the conversion table: we're using the equatorial amounts
    // so it is imprecise, but we're alright with that for testing purposes
    // 0.00001 => 1.1132 m
    var factor = (0.00001 * spacing)/1.1132;
    var get_next_position = function(lat, lon)
    {
	var nlon = lon + factor, nlat = lat;
	if(nlon > lowerright[1]){
	    nlon = topleft[1];
	    nlat = lat - factor;
	}
	//console.log(lat + " => "+nlat+"; "+lon+" => "+nlon);
	return [nlat, nlon];
    };

    var start = Date.now();
    
    var done = function(globalcount)
    {
	var n = Date.now();
	var elapsed = (n - start)/1000;
	ltps = globalcount/elapsed;
	console.log("Inserted "+globalcount+" rows; "+ltps.toFixed(2)+" total records per second.");
	console.log("Total Elapsed Time: "+elapsed.toFixed(2)+" seconds.");
    };
    
    var spotcount = 0, globalcount = 0;
    var build_database = function(err)
    {
	console.log("Using database location: "+path+".");
	console.log("Using location spacing: "+spacing+" (in meters).");
	console.log("Using location density: "+density+".");
	
	// create the database
	var levelup = require('levelup');
	var dboptions = {
            keyEncoding: 'utf8',
            valueEncoding: 'json',
            compression: false,
            cacheSize: 64*1024*1024,
            createIfMissing: true,
            disableCompact: true,
	    db: require('leveldown')
	};
	levelup(path, dboptions, function(err, ndb){
	    // save the top level settings
	    var settings = { 'topleft': { lat: topleft[0], lon: topleft[1] }, 'lowerright': { lat: lowerright[0], lon: lowerright[1] }, spacing: spacing, density: density };
	    // create the geo database
	    geo = require('../lib/level-geospatial')(ndb, { mode: 'bulk' });
	    
	    console.log("Geo database initialized.");
	    	    
	    var add_point = function(lat, lon, id, doc){
		doc['id'] = id;
		geo.put({ lat: lat, lon: lon }, doc['id'], doc, function(err){
		    if (err) console.log("error processing line "+cnt+"("+util.inspect(doc)+"): "+err);
		    else{
			++globalcount;
			++spotcount;
			if(globalcount%1000 == 0){
			    var n = Date.now();
			    ltps = globalcount/((n-start)/1000);
			    console.log("Inserted "+globalcount+" rows; "+ltps.toFixed(2)+" total records per second.");
			}
			
			var nlat = lat, nlon = lon, nid = uuid.v4();
			if(spotcount == density){
			    // see if we're done
			    if(nlat <= lowerright[0]){
				if(nlon == topleft[1] || nlon >= lowerright[1]){
				    // and we're done
				    settings['size'] = globalcount;
				    ndb.put('config', JSON.stringify(settings));
				    /*
				      ndb.get('config', function(err, val){
				      console.log(val);
				      });
				    */
				    done(globalcount);
				    return;
				}
			    }
			    
			    // move over by the appropropriate amount			    
			    var npos = get_next_position(lat, lon);
			    nlat = npos[0];
			    nlon = npos[1];

			    // reset the spot count
			    spotcount = 0;
			}
			
			setImmediate(add_point, nlat, nlon, nid, { name: "Location #"+ globalcount+"; Position #"+spotcount, pos: { lat: nlat, lon: nlon }});
		    }
		});	    
	    };

	    // and then start
	    add_point(topleft[0], topleft[1], uuid.v4(), { name: "Location #"+ globalcount+"; Position #"+spotcount, pos: { lat: topleft[0], lon: topleft[1] }});
	});
    };

    var exists = fs.existsSync(path);
    if(exists){
	if(!overwrite){
	    console.log("The specified location ("+path+") exists and is not empty: please remove the contents of that location or specify the --overwrite option.");
	    return;
	}

	var stat = fs.statSync(path);
	if(stat.isFile()){
	    fs.unlinkSync(path);
	    build_database();
	}else{
	    var rmdir = require('rimraf');
	    rmdir(path, build_database);
	}
    }else{
	build_database();
    }
})();
 
