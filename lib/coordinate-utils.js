var EarthRadius = 6378137;


module.exports.displaceLatLon = displaceLatLon;
module.exports.boundingRectangle = boundingRectangle;
module.exports.distance = distance;

function displaceLatLon(lat, lon, dy, dx){
	
	var dLat = dy / EarthRadius;
	var dLon = dx / (EarthRadius * Math.cos(radians(lat)))

 	var newLat = lat + degrees(dLat);
 	var newLon = lon + degrees(dLon);
 	return {
 		lat: newLat,
 		lon: newLon
 	}
}

function boundingRectangle(lat, lon, radius){
	var topLeft = displaceLatLon(lat,lon,-radius,-radius);
	var bottomRight = displaceLatLon(lat,lon,radius,radius);
	return {
		top: topLeft.lat,
		left: topLeft.lon,
		bottom: bottomRight.lat,
		right: bottomRight.lon
	}
}


function radians(x){
	return x * Math.PI / 180;
}

function degrees(x){
	return (x * 180) / Math.PI
}

//Haversine 
function distance(lat1, lon1, lat2, lon2) {
	var dlon = radians(lon2 - lon1);
	var dlat = radians(lat2 - lat1);

	var a = (Math.sin(dlat / 2) * Math.sin(dlat / 2)) + Math.cos(radians(lat1)) * Math.cos(radians(lat2)) * (Math.sin(dlon / 2) * Math.sin(dlon / 2));
	var angle = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	return angle * EarthRadius;
}

//Equirectangular approximation
function distanceEquiRect(lat1,lon1,lat2,lon2){
	var lat1 = radians(lat1);
    	var lon1 = radians(lon1);
    	var lat2 = radians(lat2);
    	var lon2 = radians(lon2);
    	
    	var x = (lon2 - lon1) * Math.cos((lat1 + lat2) / 2);
	var y = (lat2 - lat1);
	var d = Math.sqrt(x * x + y * y) * EarthRadius;
	
	return d;
}

//Spherical Law of Cosines approximation
function distanceSphereCos(lat1,lon1,lat2,lon2){
	var lat1 = radians(lat1);
    	var lon1 = radians(lon1);
    	var lat2 = radians(lat2);
    	var lon2 = radians(lon2);
    	
    	var d = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1)) * EarthRadius;
    	
    	return d;
}
