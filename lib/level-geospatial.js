var ts = require("./tile-system");
var cu = require("./coordinate-utils");
var assert = require("assert");
var stream = require('stream')
var Readable = stream.Readable;
var MaxDetailLevel = 22;

module.exports = function(db, opts){
	var hashPrecision = opts && 'hashPrecision' in opts ? opts.hashPrecision : 10;
	var storeByPrecision = opts && 'storeByPrecision' in opts;

	function idToKey(id, callback){
		db.get("keys~" + id, callback);
	}

	/* SLOw */
	function put(position, id, value, callback){
		var newKey = encodeLevelKey(position, id);
  	        var hashKey = storeByPrecision ? latLonToQuadKey(position, hashPrecision) : "fixed";
		if(opts && 'mode' in opts && opts.mode == 'bulk'){
			var batch = [];

			// add the main update to the batch
  		        batch.push({type:"put", key:"geos~" + newKey, value:value, hash: hashKey });
		
			// update the pointer
  		        batch.push({type:"put", key:"keys~" + id, value:newKey, hash: hashKey });	

			db.batch(batch, { hash: hashKey }, function (err) {
				if (callback){
					callback(err);
				}
			});
		}else{
		// look up old key
			idToKey(id, function(err, oldKey){

				var batch = [];

				// add the main update to the batch
			    batch.push({type:"put", key:"geos~" + newKey, value:value, hash: hashKey });
				if (oldKey){
					// remove the old key 
					batch.push({type:"del", key:"geos~" + oldKey});	
				}
				// update the pointer
			    batch.push({type:"put", key:"keys~" + id, value:newKey, hash: hashKey });	

				db.batch(batch, { hash: hashKey }, function (err) {
					if (callback){
						callback(err);
					}
				});
			});
		}
		
	}

	/* SLOW */
	function getById(id, callback){
		idToKey(id,function(err, key){
			if (err){
				callback(err);
			} else {
				db.get("geos~" + key, function(err, data){
					if (err){
						callback(err);
					} else {
						var output = decodeLevelKey("geos~" + key);
						output.value = data;
						callback(null,output);
					}

				});
			}
		});
	}

	function get(position, id, callback){
		var key = encodeLevelKey(position, id);
		var options = { 'hash': storeByPrecision ? latLonToQuadKey(position, hashPrecision) : "fixed" };
		db.get("geos~" + key, options, function(err,data){
			if (err){
				callback(err);
			} else {
				var output = decodeLevelKey("geos~" + key);
				output.value = data;
				callback(null,output);
			}

		});
	}

	/* SLOW */
	function del(id, callback){
		idToKey(id, function(err, oldKey){

			if (oldKey){
				//var decoded = decodeLevelKey(oldKey);
				//var hash = latLonToQuadKey(decoded.position, hashPrecision);
				var hash = storeByPrecision ? oldKey.substring(0, hashPrecision) : "fixed";
				var batch = [];
				batch.push({type:"del", key:"keys~" + id, hash: hash });
				batch.push({type:"del", key:"geos~" + oldKey, hash: hash});	
				db.batch(batch, function (err) {
					if (callback){
						callback(err);
					}
				});
			} else {
				if (callback){
					callback();
				}
			}
		});		
	}


	function search(position, radius, options){
	    var cache = null;
	    if('cache' in options) cache = options['cache'];
		var stream = new Readable({objectMode : true});
 	    var hash = storeByPrecision ? latLonToQuadKey(position, hashPrecision) : "fixed";
		var quadKeys = getSearchRange(position,radius);
		var openStreams = 0;
		//var total = 0;
		//var hits = 0;

		quadKeys.forEach(function(quadKey){

			//console.log("http://ak.dynamic.t1.tiles.virtualearth.net/comp/ch/" + quadKey + "?mkt=en-gb&it=G,VE,BX,L,LA&shading=hill&og=18&n=z");

			var options = {};
			if(opts && 'singular' in opts) options['singular'] = true;
			options.start = "geos~" + quadKey;
			options.end =  "geos~" + quadKey + "~";
		    options.hash = hash;
			openStreams++;

			db.createReadStream(options)
			  .on('data', function (data) {
			  	//total++;
			      if(cache && 'id' in data.value && data.value.id in cache){
				  return;
			      }

			  	var key = decodeLevelKey(data.key);
		
			  	var d = cu.distance(parseFloat(position.lat),parseFloat(position.lon),key.position.lat,key.position.lon);
				
			  	if (d <= radius){
			  		//hits++;
			  		key.distance = d;
			  		key.value = data.value;
			  		stream.push(key);		
				    if(cache && 'id' in data.value) cache[data.value.id] = key; 
			  	}

			  })
			  .on('end', function () {
			  	openStreams--;
			  	if (openStreams == 0) {
			  		stream.push(null);
			  		//console.log("stats:" + hits + "/" + total);
			  	}
			  })
			  .on('error', function(err){
			  	stream.emit('error', err);	
			  });
			stream._read = function(){};

		});

		return stream;
	}


	function searchKey(position,depth){
		var pixelCoords = ts.latLonToPixelXY(position.lat,position.lon,depth);
		var tileCoords = ts.pixelXYToTileXY(pixelCoords.pixelX,pixelCoords.pixelY);
		return ts.tileXYToQuadKey(tileCoords.tileX,tileCoords.tileY,depth);
	}

	function latLonToQuadKey(position, level){
		var pixelCoords = ts.latLonToPixelXY(position.lat,position.lon,level);
		var tileCoords = ts.pixelXYToTileXY(pixelCoords.pixelX,pixelCoords.pixelY);
		return ts.tileXYToQuadKey(tileCoords.tileX,tileCoords.tileY,level);
	}

	function encodeLevelKey(position, id){
		id = String(id).replace("~","");
		return latLonToQuadKey(position, MaxDetailLevel) + "~" + String(position.lat) + "~" + String(position.lon) + "~" + id;
	}

	function decodeLevelKey(key){
		var parts = key.split("~");
		return {
			quadKey: parts[1],
			position: {
				lat: parseFloat(parts[2]),
				lon: parseFloat(parts[3])},
			id: parts[4]
		}
	}

	function getSearchRange(position, radius){
		var box = cu.boundingRectangle(position.lat, position.lon, radius);
		var topLeft = ts.latLonToPixelXY(box.top,box.left,MaxDetailLevel);
		var bottomRight = ts.latLonToPixelXY(box.bottom,box.right,MaxDetailLevel);
		var numberOfTilesAtMaxDepth = Math.floor((bottomRight.pixelX - topLeft.pixelX) / 256);
		var zoomLevelsToRise = Math.floor(Math.log(numberOfTilesAtMaxDepth) / Math.log(2));
		zoomLevelsToRise++;

		var quadDictionary = {};
		quadDictionary[latLonToQuadKey({lat:box.top, lon:box.left},Math.max(0,MaxDetailLevel - zoomLevelsToRise))] = true;
		quadDictionary[latLonToQuadKey({lat:box.top, lon:box.right},Math.max(0,MaxDetailLevel - zoomLevelsToRise))] = true;
		quadDictionary[latLonToQuadKey({lat:box.bottom, lon:box.left},Math.max(0,MaxDetailLevel - zoomLevelsToRise))] = true;
		quadDictionary[latLonToQuadKey({lat:box.bottom, lon:box.right},Math.max(0,MaxDetailLevel - zoomLevelsToRise))] = true;

		var quadList = [];
		for (x in quadDictionary){
			quadList.push(x);
		}

		return quadList;
	}


	return { 
		put:put,
		search:search,
		getByKey:getById,
		get:get,
		del:del,
		encodeLevelKey: encodeLevelKey
	};
}
