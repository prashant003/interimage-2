/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.geometry;

import java.util.HashMap;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Converts between LatLong and Web Mercator coordinate systems.<br>
 * @author Rodrigo Ferreira
 */
public class WebMercatorLatLongConverter {
	
	private double _equatorialRadius;
	
	private final double RAD = Math.PI / 180.0;
	
	private final Map<String, double[]> _datumTable = new HashMap<String, double[]>();
	
	public WebMercatorLatLongConverter() {		
		/* Initialize datums */
		_datumTable.put("WGS84", new double[] {6378137.0, 298.257223563});		
	}
	
	public void setDatum(String datum) {
		_equatorialRadius = _datumTable.get(datum)[0];		
	}
	
	public double[] getDatum(String datum) {
		return _datumTable.get(datum);		
	}
	
	public boolean hasDatum(String datum) {
		return _datumTable.containsKey(datum);		
	}
	
	public double getEquatorialRadius() {
		return _equatorialRadius;
	}
	
	public void LatLongToWebMercator(Coordinate coordinate) {

		double lat = coordinate.y;
		double lngd = coordinate.x;
		
		double phi = lat * RAD;
		double lng = lngd * RAD;
				
		double x = _equatorialRadius * lng;
		double y = (_equatorialRadius/2.0) * Math.log( (Math.sin(phi)+1.0) / (1.0-Math.sin(phi) ));
		
		coordinate.x = x;
		coordinate.y = y;
		
	}
	
	public void WebMercatorToLatLong(Coordinate coordinate) {
		double y = coordinate.y;
		double x = coordinate.x;
		
		double lng = x / _equatorialRadius;
		double lngd = lng / RAD;
		
		double phi = Math.asin((1.0 / (-(Math.pow(Math.E, ((2.0*y) / _equatorialRadius)) + 1.0) /2.0)) + 1.0); 
		double lat = phi / RAD;
		
		coordinate.x = lngd;
		coordinate.y = lat;
		
	}
	
}
