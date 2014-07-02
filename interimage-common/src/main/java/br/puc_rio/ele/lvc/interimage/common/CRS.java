package br.puc_rio.ele.lvc.interimage.common;

import java.util.HashMap;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.Geometry;

public class CRS {
	
	Map<String, double[]> _bounds;
	
	public CRS() {
		_bounds = new HashMap<String,double[]>();
		
		//_bounds.put("Web Mercator", new double[] {-20026376.39, 20048966.10, 20026376.39, -20048966.10});
		_bounds.put("UTMN", new double[] {166021.4431, 0.0000, 833978.5569, 9329005.1825});
		_bounds.put("UTMS", new double[] {166021.4431, 1116915.0440, 833978.5569, 10000000.0000});
		_bounds.put("LongLat", new double[] {-180, -90, 180, 90});
		
	}
	
	public static void convert(String crsFrom, String crsTo, Geometry geometry) {
        
		if (crsFrom != null) {
			if (crsFrom.isEmpty()) {
				return;
			}
		} else {
			return;
		}
		
		if (crsTo != null) {
			if (crsTo.isEmpty()) {
				return;
			}
		} else {
			return;
		}
		
		if (crsFrom.equals(crsTo))
			return;
		
		int crsFromCode = Integer.parseInt(crsFrom.split(":")[1]);					
		int crsToCode = Integer.parseInt(crsTo.split(":")[1]);
				
		if (((crsFromCode >= 32601) && (crsFromCode <= 32660)) || ((crsFromCode >= 32701) && (crsFromCode <= 32760))) {	//from UTM
			
			final int utmZone = (crsFromCode>32700) ? crsFromCode-32700 : crsFromCode-32600;
			final boolean southern = (crsFromCode>32700) ? true : false;
							
			if (crsToCode == 4326) {	//to Longlat
				
				final UTMLatLongConverter converter = new UTMLatLongConverter();
				converter.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter.UTMToLatLong(coord, utmZone, southern);
	                }
	            });
				
				geometry.setSRID(crsToCode);					
				geometry.geometryChanged();
				
			} else if (crsToCode == 3857) {	//To Web Mercator
				
				final UTMLatLongConverter converter = new UTMLatLongConverter();
				converter.setDatum("WGS84");
				
				final WebMercatorLatLongConverter converter2 = new WebMercatorLatLongConverter();
				converter2.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter.UTMToLatLong(coord, utmZone, southern);
	                	converter2.LatLongToWebMercator(coord);
	                }
	            });
				
				geometry.setSRID(crsToCode);					
				geometry.geometryChanged();
				
			}
		} else if (crsFromCode == 4326) {	//from Longlat
			
			if (((crsToCode >= 32601) && (crsToCode <= 32660)) || ((crsToCode >= 32701) && (crsToCode <= 32760))) {	//to UTM
							
				final UTMLatLongConverter converter = new UTMLatLongConverter();
				converter.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter.LatLongToUTM(coord);
	                }
	            });
				
				geometry.setSRID(crsToCode);					
				geometry.geometryChanged();
				
			} else if (crsToCode == 3857) {	//to Web Mercator
				
				final WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
				converter.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter.LatLongToWebMercator(coord);
	                }
	            });
				
				geometry.setSRID(crsToCode);					
				geometry.geometryChanged();
				
			}
			
		} else if (crsFromCode == 3857) {	//from Web Mercator
		
			if (((crsToCode >= 32601) && (crsToCode <= 32660)) || ((crsToCode >= 32701) && (crsToCode <= 32760))) {	//to UTM
			
				final UTMLatLongConverter converter = new UTMLatLongConverter();
				converter.setDatum("WGS84");
				
				final WebMercatorLatLongConverter converter2 = new WebMercatorLatLongConverter();
				converter2.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter2.WebMercatorToLatLong(coord);
	                	converter.LatLongToUTM(coord);		                	
	                }
	            });
				
				geometry.setSRID(crsToCode);					
				geometry.geometryChanged();
				
			} else if (crsToCode == 4326) {	//to Longlat
				
				final WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
				converter.setDatum("WGS84");
				
				geometry.apply(new CoordinateFilter() {
	                public void filter(Coordinate coord) {
	                	converter.WebMercatorToLatLong(coord);
	                }
	            });
				
				geometry.setSRID(crsToCode);
				geometry.geometryChanged();
				
			}
			
		} else {
			//throw new Exception("CRS code " + crsFromCode + " not supported");
		}
				
	}
		
	public double[] getBounds(String crs) {
		
		int crsCode = Integer.parseInt(crs.split(":")[1]);
		
		String crsName = null;
		
		if (((crsCode >= 32601) && (crsCode <= 32660)) || ((crsCode >= 32701) && (crsCode <= 32760))) {	//from UTM
			
			//int utmZone = (crsCode>32700) ? crsCode-32700 : crsCode-32600;
			crsName = (crsCode>32700) ? "UTMS" : "UTMN";
		
		} else if (crsCode == 4326) {
			crsName = "LongLat";
		}
			
		return _bounds.get(crsName);
	}
	
}
