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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF that combines several bags into one.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geometry, data, properties);<br>
 * 		B = load 'mydata2' as (geometry, data, properties);<br>
 * 		C = cogroup A by properties#'tile', B by properties#'tile';<br>
 * 		D = flatten(Combine(A,B));
 * @author Rodrigo Ferreira
 * 
 */
@Deprecated
public class Combine extends EvalFunc<DataBag> {
		
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * the columns are assumed to have the bags
     * @exception java.io.IOException
     * @return a bag with the input bags combined
     */
	@SuppressWarnings("rawtypes")
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
		
		try {
						
			DataBag output = BagFactory.getInstance().newDefaultBag();
			
			for (int i=0; i<input.size(); i++) {
				DataBag bag = DataType.toBag(input.get(i));				
				Iterator it = bag.iterator();
		        while (it.hasNext()) {
		        	Tuple t = (Tuple)it.next();
		        	output.add(t);
		        }
			}
			
			return output;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		
		try {

			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			list.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
			
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			Schema bagSchema = new Schema(ts);
			
			Schema.FieldSchema bs = new Schema.FieldSchema(null, bagSchema, DataType.BAG);
			
			return new Schema(bs);

		} catch (Exception e) {
			return null;
		}
		
    }
	
}
