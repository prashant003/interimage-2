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
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF that creates a new data map.<br><br>
 * Example:<br>
 * 		A = load 'mydata';<br>
 * 		B = foreach A generate CreateData('');
 * @author Rodrigo Ferreira
 *
 */
public class CreateData extends EvalFunc<Map<String,Object>> {
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the data values
     * @exception java.io.IOException
     * @return new map
     * //TODO: implement this UDF properly
     */
	@Override
	public Map<String,Object> exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
            return null;
        
		try {
			
			Map<String,Object> props = new HashMap<String,Object>();
			props.put("0", "");
			return props;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }
	
}
