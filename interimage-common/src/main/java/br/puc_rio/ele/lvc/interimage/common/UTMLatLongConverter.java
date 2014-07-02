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

package br.puc_rio.ele.lvc.interimage.common;

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

	private final double K0 = 0.9996;
	private final double RAD = Math.PI / 180.0;
	private final double DEG = 180.0 / Math.PI;
	
	private final Map<String, double[]> _datumTable = new HashMap<String, double[]>();
	
	public UTMLatLongConverter() {
		/* Initialize datums */
		_datumTable.put("WGS84", new double[] {6378137.0, 298.257223563});		
	}
	
	public void setDatum(String datum) {
		_equatorialRadius = _datumTable.get(datum)[0];
        _polarFlattening = 1.0 / _datumTable.get(datum)[1];
        _polarRadius = _equatorialRadius * (1.0 - _polarFlattening);   // polar radius
        _eccentricity = Math.sqrt(1.0 - Math.pow(_polarRadius, 2.0) / Math.pow(_equatorialRadius, 2.0));
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
	
	public double getPolarFlattening() {
		return _polarFlattening;
	}
	
	public double getPolarRadius() {
		return _polarRadius;
	}
	
	public double getEccentricity() {
		return _eccentricity;
	}

	public void LatLongToUTM(Coordinate coordinate) {
		
		//converts lat/long to UTM coords.  Equations from USGS Bulletin 1532 
		//East Longitudes are positive, West longitudes are negative. 
		//North latitudes are positive, South latitudes are negative
		//Lat and Long are in decimal degrees
		//Written by Chuck Gantz- chuck.gantz@globalstar.com

		double Lat = coordinate.y;
		double Long = coordinate.x;
		
		double a = _equatorialRadius;
		double eccSquared = _eccentricity*_eccentricity;	

		double LongOrigin;
		double eccPrimeSquared;
		double N, T, C, A, M;
		
		//Make sure the longitude is between -180.00 .. 179.9
		double LongTemp = (Long+180)-((int)((Long+180)/360))*360-180; // -180.00 .. 179.9;

		double LatRad = Lat*RAD;
		double LongRad = LongTemp*RAD;
		double LongOriginRad;
		int    ZoneNumber;

		ZoneNumber = ((int)((LongTemp + 180)/6)) + 1;
	  
		if( Lat >= 56.0 && Lat < 64.0 && LongTemp >= 3.0 && LongTemp < 12.0 )
			ZoneNumber = 32;

	  // Special zones for Svalbard
		if( Lat >= 72.0 && Lat < 84.0 ) 
		{
		  if(      LongTemp >= 0.0  && LongTemp <  9.0 ) ZoneNumber = 31;
		  else if( LongTemp >= 9.0  && LongTemp < 21.0 ) ZoneNumber = 33;
		  else if( LongTemp >= 21.0 && LongTemp < 33.0 ) ZoneNumber = 35;
		  else if( LongTemp >= 33.0 && LongTemp < 42.0 ) ZoneNumber = 37;
		 }
		LongOrigin = (ZoneNumber - 1)*6 - 180 + 3;  //+3 puts origin in middle of zone
		LongOriginRad = LongOrigin * RAD;

		//compute the UTM Zone from the latitude and longitude
		//sprintf(UTMZone, "%d%c", ZoneNumber, UTMLetterDesignator(Lat));

		eccPrimeSquared = (eccSquared)/(1-eccSquared);

		N = a/Math.sqrt(1-eccSquared*Math.sin(LatRad)*Math.sin(LatRad));
		T = Math.tan(LatRad)*Math.tan(LatRad);
		C = eccPrimeSquared*Math.cos(LatRad)*Math.cos(LatRad);
		A = Math.cos(LatRad)*(LongRad-LongOriginRad);

		M = a*((1	- eccSquared/4		- 3*eccSquared*eccSquared/64	- 5*eccSquared*eccSquared*eccSquared/256)*LatRad 
					- (3*eccSquared/8	+ 3*eccSquared*eccSquared/32	+ 45*eccSquared*eccSquared*eccSquared/1024)*Math.sin(2*LatRad)
										+ (15*eccSquared*eccSquared/256 + 45*eccSquared*eccSquared*eccSquared/1024)*Math.sin(4*LatRad) 
										- (35*eccSquared*eccSquared*eccSquared/3072)*Math.sin(6*LatRad));
		
		double UTMEasting = (double)(K0*N*(A+(1-T+C)*A*A*A/6
						+ (5-18*T+T*T+72*C-58*eccPrimeSquared)*A*A*A*A*A/120)
						+ 500000.0);

		double UTMNorthing = (double)(K0*(M+N*Math.tan(LatRad)*(A*A/2+(5-T+9*C+4*C*C)*A*A*A*A/24
					 + (61-58*T+T*T+600*C-330*eccPrimeSquared)*A*A*A*A*A*A/720)));
		if(Lat < 0)
			UTMNorthing += 10000000.0; //10000000 meter offset for southern hemisphere
		
		coordinate.x = UTMEasting;
		coordinate.y = UTMNorthing;
		
	}
	
	public void UTMToLatLong(Coordinate coordinate, int ZoneNumber, boolean southern) {
		
		//converts UTM coords to lat/long.  Equations from USGS Bulletin 1532 
		//East Longitudes are positive, West longitudes are negative. 
		//North latitudes are positive, South latitudes are negative
		//Lat and Long are in decimal degrees. 
		//Written by Chuck Gantz- chuck.gantz@globalstar.com

		double UTMEasting = coordinate.x;
		double UTMNorthing = coordinate.y;
		
		double k0 = 0.9996;
		double a = _equatorialRadius;
		double eccSquared = _eccentricity*_eccentricity;
		double eccPrimeSquared;
		double e1 = (1-Math.sqrt(1-eccSquared))/(1+Math.sqrt(1-eccSquared));
		double N1, T1, C1, R1, D, M;
		double LongOrigin;
		double mu, phi1Rad;
		double x, y;
		//double phi1;
		//int ZoneNumber;
		//char* ZoneLetter;
		//int NorthernHemisphere; //1 for northern hemispher, 0 for southern

		x = UTMEasting - 500000.0; //remove 500,000 meter offset for longitude
		y = UTMNorthing;

		//ZoneNumber = strtoul(UTMZone, &ZoneLetter, 10);
		//if(!southern)
			//NorthernHemisphere = 1;//point is in northern hemisphere
		//else
		if (southern) {
			//NorthernHemisphere = 0;//point is in southern hemisphere
			y -= 10000000.0;//remove 10,000,000 meter offset used for southern hemisphere
		}

		LongOrigin = (ZoneNumber - 1)*6 - 180 + 3;  //+3 puts origin in middle of zone

		eccPrimeSquared = (eccSquared)/(1-eccSquared);

		M = y / k0;
		mu = M/(a*(1-eccSquared/4-3*eccSquared*eccSquared/64-5*eccSquared*eccSquared*eccSquared/256));

		phi1Rad = mu	+ (3*e1/2-27*e1*e1*e1/32)*Math.sin(2*mu) 
					+ (21*e1*e1/16-55*e1*e1*e1*e1/32)*Math.sin(4*mu)
					+(151*e1*e1*e1/96)*Math.sin(6*mu);
		//phi1 = phi1Rad*DEG;

		N1 = a/Math.sqrt(1-eccSquared*Math.sin(phi1Rad)*Math.sin(phi1Rad));
		T1 = Math.tan(phi1Rad)*Math.tan(phi1Rad);
		C1 = eccPrimeSquared*Math.cos(phi1Rad)*Math.cos(phi1Rad);
		R1 = a*(1-eccSquared)/Math.pow(1-eccSquared*Math.sin(phi1Rad)*Math.sin(phi1Rad), 1.5);
		D = x/(N1*k0);

		Double Lat = phi1Rad - (N1*Math.tan(phi1Rad)/R1)*(D*D/2-(5+3*T1+10*C1-4*C1*C1-9*eccPrimeSquared)*D*D*D*D/24
						+(61+90*T1+298*C1+45*T1*T1-252*eccPrimeSquared-3*C1*C1)*D*D*D*D*D*D/720);
		Lat = Lat * DEG;

		Double Long = (D-(1+2*T1+C1)*D*D*D/6+(5-2*C1+28*T1-3*C1*C1+8*eccPrimeSquared+24*T1*T1)
						*D*D*D*D*D/120)/Math.cos(phi1Rad);
		Long = LongOrigin + Long * DEG;
	
		coordinate.x = Long;
		coordinate.y = Lat;
		
	}
		
}
