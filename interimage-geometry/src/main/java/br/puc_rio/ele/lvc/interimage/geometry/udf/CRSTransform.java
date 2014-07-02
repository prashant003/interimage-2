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

import br.puc_rio.ele.lvc.interimage.common.CRS;
import br.puc_rio.ele.lvc.interimage.common.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that re-projects a geometry according to the given coordinate systems.<br>
 * So far, the supported coordinate systems are: EPSG:4326 (WGS 84), EPSG:32601-EPSG:32660 (WGS 84 / UTM Zones N), EPSG:32701-EPSG:32760 (WGS 84 / UTM Zones S) and EPSG:3857 (Web Mercator).<br><br>
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
     * 
     * TODO: deal with data?
     */
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
				
		try {
			Geometry geometry = _geometryParser.parseGeometry(input.get(0));
			
			String crsFrom = DataType.toString(input.get(1));
			String crsTo = DataType.toString(input.get(2));

			CRS.convert(crsFrom, crsTo, geometry);
						
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
