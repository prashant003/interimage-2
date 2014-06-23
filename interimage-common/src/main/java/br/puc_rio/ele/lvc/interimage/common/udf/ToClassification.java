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
 * A UDF that updates the classification map with the given class and membership.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (props);<br>
 * 		B = foreach A generate ToClassification('class', membership, props) as props;
 * @author Rodrigo Ferreira
 *
 */
public class ToClassification extends EvalFunc<Map<String,Object>> {
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple; first column is assumed to have the class name<br>
     * second column is assumed to have the membership value<br>
     * third column is assumed to have the properties map
     * @exception java.io.IOException
     * @return map with the given classification
     */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String,Object> exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {
			String className = DataType.toString(input.get(0));
			Double membership = DataType.toDouble(input.get(1));			
			Map<String,Object> objProperties = DataType.toMap(input.get(2));
			
			Map<String,Double> classification = null;
			
			if (objProperties.containsKey("classification")) {
				classification = (Map<String,Double>)objProperties.get("classification");
				classification.put(className,membership);
			} else {
				classification = new HashMap<String,Double>();
				classification.put(className,membership);
			}
			
			objProperties.put("classification", classification);
			
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
