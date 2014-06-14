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

import org.apache.pig.data.DataByteArray;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ByteArrayInStream;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Retrieves a geometry from a pig attribute. It automatically detects its format
 * and tries to get the geometry from it. It understands WKT (text) and WKB (binary) formats.<br><br>
 * 
 * In particular, here are the checks done in order:<br>
 * 1 - DataByteArray (WKB)<br>
 * 2 - String (WKT)<br>
 * 3 - Hexadecimal String (WKB)<br>
 * 
 * @author Rodrigo Ferreira
 *
 */
public class GeometryParser {

	/**
     * Method that parses a geometry object.
     * @param geometry object
     * @return geometry, or null in case of processing error
     */
	public Geometry parseGeometry(Object objGeometry) {
		
		Geometry geometry = null;
		Object obj = objGeometry;
		
		if (obj instanceof DataByteArray) {
					
			try {
				//parsing as a well known binary (WKB)
				byte[] arrGeometry = ((DataByteArray)obj).get();
				geometry = new WKBReader().read(new ByteArrayInStream(arrGeometry));
			} catch (Exception e) {
				//parsing as an encoded well known text (WKT)
		        obj = new String(((DataByteArray) obj).get());
			}
			
		}
		
		if (obj instanceof String) {
			
			try {
				//parsing as a well known text (WKT)
				String strGeometry = (String)obj;
				geometry = new WKTReader().read(strGeometry);
			} catch (Exception e1) {
				try {
					//parsing as a hexadecimal string					
					byte[] arrGeometry = WKBReader.hexToBytes((String)obj);
					geometry = new WKBReader().read(new ByteArrayInStream(arrGeometry));
				} catch (Exception e2) {
					System.err.println("Failed to parse geometry; error - " + e2.getMessage());
					//cannot parse it. Returning null
				}
			}
			
		}
		
		return geometry;
		
	}
	
}
