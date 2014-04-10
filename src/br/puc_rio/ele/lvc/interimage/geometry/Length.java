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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that returns the length of a geometry.
 * Example:
 * 		A = load 'mydata' as (geometry);
 * 		B = foreach A generate Length(geometry);
 * @author Rodrigo Ferreira
 *
 */
public class Length extends EvalFunc<Double> {
	
	private final GeometryParser geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple; first column is assumed to have the geometry
     * @exception java.io.IOException
     * @return length of the geometry
     */
	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {
			Object objGeometry = input.get(0);
			Geometry geometry = geometryParser.parseGeometry(objGeometry);
			return geometry.getLength();
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}
