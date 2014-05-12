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
 * Converts between LatLong and UTM coordinate systems.<br>
 * @author Rodrigo Ferreira
 */
public class UTMLatLongConverter {

	private double _equatorialRadius;
	private double _polarFlattening;
	private double _polarRadius;
	private double _eccentricity;
	@SuppressWarnings("unused")
	private double _e;
	
	private final double K = 1.0;
	private final double K0 = 0.9996;
	private final double RAD = Math.PI / 180.0;
	
	private final Map<String, double[]> _datumTable = new HashMap<String, double[]>();
	
	public UTMLatLongConverter() {
		/* Initialize datums */
		_datumTable.put("WGS84", new double[] {6378137.0, 298.2572236});		
	}
	
	public void setDatum(String datum) {
		_equatorialRadius = _datumTable.get(datum)[0];
        _polarFlattening = 1.0 / _datumTable.get(datum)[1];
        _polarRadius = _equatorialRadius * (1.0 - _polarFlattening);   // polar radius
        _eccentricity = Math.sqrt(1.0 - Math.pow(_polarRadius, 2.0) / Math.pow(_equatorialRadius, 2.0));
        _e = _eccentricity / Math.sqrt(1.0 - Math.pow(_eccentricity, 1.0));
	}
	
	public void LatLongToUTM(Coordinate coordinate) {
		
		double lat = coordinate.y;
		double lngd = coordinate.x;
		
		double phi = lat * RAD;                              // convert latitude to radians
        //double lng = lngd * RAD;                             // convert longitude to radians
        double utmz = 1.0 + Math.floor((lngd + 180) / 6);            // longitude to utm zone
        double zcm = 3.0 + 6.0 * (utmz - 1.0) - 180.0;                     // central meridian of a zone        
		//int latz = 0;                                           // this gives us zone A-B for below 80S
        double esq = (1.0 - (_polarRadius / _equatorialRadius) * (_polarRadius / _equatorialRadius));
        double e0sq = _eccentricity * _eccentricity / (1.0 - Math.pow(_eccentricity, 2.0));
        double M = 0.0;

        // convert latitude to latitude zone for nato
        /*if (lat > -80 && lat < 72) {
            latz = Math.floor((lat + 80) / 8) + 2;      // zones C-W in this range
        } if (lat > 72 && lat < 84) {
            latz = 21;                                  // zone X
        } else if (lat > 84) {
            latz = 23;                                  // zone Y-Z
        }*/

        double N = _equatorialRadius / Math.sqrt(1.0 - Math.pow(_eccentricity * Math.sin(phi), 2.0));
        double T = Math.pow(Math.tan(phi), 2.0);
        double C = e0sq * Math.pow(Math.cos(phi), 2.0);
        double A = (lngd - zcm) * RAD * Math.cos(phi);

        // calculate M (USGS style)
        M = phi * (1.0 - esq * (1.0 / 4.0 + esq * (3.0 / 64.0 + 5.0 * esq / 256.0)));
        M = M - Math.sin(2.0 * phi) * (esq * (3.0 / 8.0 + esq * (3.0 / 32.0 + 45.0 * esq / 1024.0)));
        M = M + Math.sin(4.0 * phi) * (esq * esq * (15.0 / 256.0 + esq * 45.0 / 1024.0));
        M = M - Math.sin(6.0 * phi) * (esq * esq * esq * (35.0 / 3072.0));
        M = M * _equatorialRadius;                                      //Arc length along standard meridian

        double M0 = 0;                                         // if another point of origin is used than the equator

        // now we are ready to calculate the UTM values...
        // first the easting
        double x = K0 * N * A * (1 + A * A * ((1 - T + C) / 6.0 + A * A * (5 - 18 * T + T * T + 72 * C - 58 * e0sq) / 120.0)); //Easting relative to CM
        x = x + 500000; // standard easting

        // now the northing
        double y = K0 * (M - M0 + N * Math.tan(phi) * (A * A * (1.0 / 2.0 + A * A * ((5 - T + 9 * C + 4 * C * C) / 24.0 + A * A * (61 - 58 * T + T * T + 600 * C - 330 * e0sq) / 720.0))));    // first from the equator
        //double yg = y + 10000000;  //yg = y global, from S. Pole
        if (y < 0) {
            y = 10000000 + y;   // add in false northing if south of the equator
        }
		
        //boolean south = phi < 0;
        
        coordinate.x = ((double)Math.round(10*(x)))/10.0;
        coordinate.y = ((double)Math.round(10*y))/10.0;
        
	}
	
	public void UTMToLatLong(Coordinate coordinate, int utmz, boolean southern) {
		
		double x = coordinate.x;
		double y = coordinate.y;
		
		double esq = (1.0 - (_polarRadius / _equatorialRadius) * (_polarRadius / _equatorialRadius));
        double e0sq = _eccentricity * _eccentricity / (1.0 - Math.pow(_eccentricity, 2.0));
        double zcm = 3.0 + 6.0 * (utmz - 1.0) - 180.0;                         // Central meridian of zone
        double e1 = (1.0 - Math.sqrt(1.0 - Math.pow(_eccentricity, 2.0))) / (1.0 + Math.sqrt(1.0 - Math.pow(_eccentricity, 2.0)));
        double M0 = 0;
        double M = 0;

        if (!southern)
            M = M0 + y / K0;    // Arc length along standard meridian. 
        else
            M = M0 + (y - 10000000) / K;

        double mu = M / (_equatorialRadius * (1 - esq * (1.0 / 4.0 + esq * (3.0 / 64.0 + 5 * esq / 256.0))));
        double phi1 = mu + e1 * (3.0 / 2.0 - 27.0 * e1 * e1 / 32.0) * Math.sin(2.0 * mu) + e1 * e1 * (21.0 / 16.0 - 55.0 * e1 * e1 / 32.0) * Math.sin(4.0 * mu);   //Footprint Latitude
        phi1 = phi1 + e1 * e1 * e1 * (Math.sin(6.0 * mu) * 151.0 / 96.0 + e1 * Math.sin(8.0 * mu) * 1097.0 / 512.0);
        double C1 = e0sq * Math.pow(Math.cos(phi1), 2.0);
        double T1 = Math.pow(Math.tan(phi1), 2.0);
        double N1 = _equatorialRadius / Math.sqrt(1.0 - Math.pow(_eccentricity * Math.sin(phi1), 2.0));
        double R1 = N1 * (1.0 - Math.pow(_eccentricity, 2.0)) / (1.0 - Math.pow(_eccentricity * Math.sin(phi1), 2.0));
        double D = (x - 500000.0) / (N1 * K0);
        double phi = (D * D) * (1.0 / 2.0 - D * D * (5.0 + 3.0 * T1 + 10.0 * C1 - 4.0 * C1 * C1 - 9.0 * e0sq) / 24.0);
        phi = phi + Math.pow(D, 6.0) * (61.0 + 90.0 * T1 + 298.0 * C1 + 45.0 * T1 * T1 - 252.0 * e0sq - 3.0 * C1 * C1) / 720.0;
        phi = phi1 - (N1 * Math.tan(phi1) / R1) * phi;

        double lat = Math.floor(1000000.0 * phi / RAD) / 1000000.0;
        double lng = D * (1 + D * D * ((-1 - 2 * T1 - C1) / 6.0 + D * D * (5 - 2 * C1 + 28 * T1 - 3 * C1 * C1 + 8 * e0sq + 24 * T1 * T1) / 120.0)) / Math.cos(phi1);
        lng = zcm + lng / RAD;

        coordinate.x = lng;
        coordinate.y = lat;
		
	}
	
}
