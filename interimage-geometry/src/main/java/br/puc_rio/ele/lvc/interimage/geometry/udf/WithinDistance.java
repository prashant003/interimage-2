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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that tests whether two geometries are within a distance.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom);<br>
 * 		B = load 'mydata2' as (geom);<br>
 * 		C = SpatialJoin(A,B,2);<br>
 * 		D = filter C by WithinDistance(A::geom,B::geom,100);<br>
 * @author Rodrigo Ferreira
 *
 */
public class WithinDistance extends EvalFunc<Boolean> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry<br>
     * second column is assumed to have a geometry<br>
     * third column is assumed to have the distance
     * @exception java.io.IOException
     * @return boolean value
     */
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
            return null;
        
		try {			
			Object objGeometry1 = input.get(0);
			Object objGeometry2 = input.get(1);
			Double distance = DataType.toDouble(input.get(2));
			Geometry geometry1 = _geometryParser.parseGeometry(objGeometry1);
			Geometry geometry2 = _geometryParser.parseGeometry(objGeometry2);
			return geometry1.isWithinDistance(geometry2, distance);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
