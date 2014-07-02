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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that replicates boundary polygons.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geometry, data, properties);<br>
 * 		B = foreach A generate flatten(Replicate(geometry, data, properties)) as (geometry, data, properties);
 * @author Rodrigo Ferreira
 *
 */
public class Replicate extends EvalFunc<DataBag> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry<br>
     * second column is assumed to have the data<br>
     * third column is assumed to have the properties
     * @exception java.io.IOException
     * @return bag
     */
	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {
						
			Object objGeometry = input.get(0);
			Map<String,String> data = (Map<String,String>)input.get(1);
			Map<String,Object> properties = DataType.toMap(input.get(2));
			
			Geometry geometry = _geometryParser.parseGeometry(objGeometry);
			
			String tileStr = DataType.toString(properties.get("tile"));
						
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
			String[] list = tileStr.split(",");
			
			/*Compute the lowest ID*/
			long min = Long.MAX_VALUE;
			for (int i=0; i<list.length; i++) {
				long id = Long.parseLong(list[i].substring(1));
				if (id < min)
					min = id;
			}
						
			for (int i=0; i<list.length; i++) {
								
				byte[] bytes = new WKBWriter().write(geometry);
    			
				Map<String,Object> props = new HashMap<String,Object>(properties);
				
				props.put("tile", list[i]);
				
				long tile = Long.parseLong(list[i].substring(1));
				
				/*Only the minimum tile instance should remain*/
				if (tile != min) {
					props.put("iirep","true");
				}
				
    			Tuple t = TupleFactory.getInstance().newTuple(3);
    			t.set(0,new DataByteArray(bytes));
    			t.set(1,new HashMap<String,String>(data));
    			t.set(2,props);
    			bag.add(t);
				
			}
			
			return bag;
			
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
