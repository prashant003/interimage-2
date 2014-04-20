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

import java.lang.Exception;

import org.apache.pig.data.DataType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Retrieves a geometry from a pig attribute. It detects the type of the column
 * and the data stored in that column and automatically detects its format
 * and tries to get the geometry from it. It understands WKT (text) and WKB (binary) formats.<br><br>
 * 
 * In particular, here are the checks done in order:<br>
 * TODO Describe input checking logic
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
		
		try {
			//Assuming WKT format
			//TODO For the binary format (WKB), an if should be created
			String strGeometry = DataType.toString(objGeometry);
			Geometry geometry = new WKTReader().read(strGeometry);
			return geometry;
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}
	
}
