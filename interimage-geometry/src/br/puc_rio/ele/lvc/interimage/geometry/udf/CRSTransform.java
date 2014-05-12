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

package br.puc_rio.ele.lvc.interimage.geometry.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.UTMLatLongConverter;
import br.puc_rio.ele.lvc.interimage.geometry.WebMercatorLatLongConverter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that re-projects a geometry according to the given coordinate systems.<br>
 * So far, the supported coordinate systems are: EPSG:4326 (WGS 84), EPSG:32601-EPSG:32660 (WGS 84 / UTM Zones N), EPSG:32701-EPSG:32760 (WGS 84 / UTM Zones S) and EPSG:3395 (World Mercator).<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom);<br>
 * 		B = foreach A generate CRSTransform(geom,'EPSG:32723','EPSG:4326');<br>
 * @author Rodrigo Ferreira
 *
 */
public class CRSTransform extends EvalFunc<DataByteArray> {
	
	private final GeometryParser _geometryParser = new GeometryParser();	
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry<br>
     * second column is assumed to have the from coordinate system<br>
     * third column is assumed to have the to coordinate system<br>
     * @exception java.io.IOException
     * @return re-projected geometry
     */
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
				
		try {
			Geometry geometry = _geometryParser.parseGeometry(input.get(0));
			
			String epsgFrom = DataType.toString(input.get(1));
			String epsgTo = DataType.toString(input.get(2));
	        
			int epsgFromCode = Integer.parseInt(epsgFrom.split(":")[1]);					
			int epsgToCode = Integer.parseInt(epsgTo.split(":")[1]);
						
			if (((epsgFromCode >= 32601) && (epsgFromCode <= 32660)) || ((epsgFromCode >= 32701) && (epsgFromCode <= 32760))) {	//from UTM
				
				final int utmZone = (epsgFromCode>32700) ? epsgFromCode-32700 : epsgFromCode-32600;
				final boolean southern = (epsgFromCode>32700) ? true : false;
								
				if (epsgToCode == 4326) {	//to Longlat
					
					final UTMLatLongConverter converter = new UTMLatLongConverter();
					converter.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	converter.UTMToLatLong(coord, utmZone, southern);
		                }
		            });
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();
					
				} else if (epsgToCode == 3395) {	//To World Mercator
					
					/*final UTMLatLongConverter converter = new UTMLatLongConverter();
					converter.setDatum("WGS84");
					
					final WorldMercatorLatLongConverter converter2 = new WorldMercatorLatLongConverter();
					converter2.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	converter.UTMToLatLong(coord, utmZone, southern);
		                	converter2.LatLongToWorldMercator(coord);
		                }
		            });
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();*/
					
					throw new Exception("EPSG code " + epsgToCode + " not supported");
					
				} else if (epsgToCode == 3857) {	//To Web Mercator
					
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
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();
					
				}
			} else if (epsgFromCode == 4326) {	//from Longlat
				
				if (((epsgToCode >= 32601) && (epsgToCode <= 32660)) || ((epsgToCode >= 32701) && (epsgToCode <= 32760))) {	//to UTM
								
					final UTMLatLongConverter converter = new UTMLatLongConverter();
					converter.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	converter.LatLongToUTM(coord);
		                }
		            });
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();
					
				} else if (epsgToCode == 3395) {	//to World Mercator
					
					throw new Exception("EPSG code " + epsgToCode + " not supported");
					
					/*final WorldMercatorLatLongConverter converter = new WorldMercatorLatLongConverter();
					converter.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	converter.LatLongToWorldMercator(coord);
		                }
		            });
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();*/
					
				} else if (epsgToCode == 3857) {	//to Web Mercator
					
					final WebMercatorLatLongConverter converter = new WebMercatorLatLongConverter();
					converter.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	converter.LatLongToWebMercator(coord);
		                }
		            });
					
					geometry.setSRID(epsgToCode);					
					geometry.geometryChanged();
					
				}
				
			} else {
				throw new Exception("EPSG code " + epsgFromCode + " not supported");
			}
						
			return new DataByteArray(new WKBWriter().write(geometry));
			
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY));
    }
	
}
