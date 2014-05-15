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

package br.puc_rio.ele.lvc.interimage.common.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF that computes the greater of two numbers.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (attrib1, attrib2);<br>
 * 		B = foreach A generate Max(attrib1, attrib2);<br>
 * @author Rodrigo Ferreira
 *
 */
public class Max extends EvalFunc<Double> {
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple; first column is assumed to have a number<br>
     * second column is assumed to have a number
     * @exception java.io.IOException
     * @return minimum value
     */
	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
            return null;
        
		try {
			Double first = DataType.toDouble(input.get(0));
			Double second = DataType.toDouble(input.get(1));
			return Math.max(first,second);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}
