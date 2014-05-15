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
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF that updates the properties map with the given field.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (field, props);<br>
 * 		B = foreach A generate ToProps(field, 'name', props) as props;<br>
 * @author Rodrigo Ferreira
 *
 */
public class ToProps extends EvalFunc<Map<String,Object>> {
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple; first column is assumed to have the value<br>
     * second column is assumed to have the field name<br>
     * third column is assumed to have the properties map
     * @exception java.io.IOException
     * @return map with the given field
     */
	@Override
	public Map<String,Object> exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {
			Object objField = input.get(0);
			String name = DataType.toString(input.get(1));
			Map<String,Object> objProperties = DataType.toMap(input.get(2));
			
			objProperties.put(name,objField);
			
			return objProperties;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }
	
}
