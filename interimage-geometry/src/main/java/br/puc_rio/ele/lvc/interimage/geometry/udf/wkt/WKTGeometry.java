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

package br.puc_rio.ele.lvc.interimage.geometry.udf.wkt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.UUID;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that parses a WKT geometry.<br><br>
 * Example:<br>
 * 		A = load 'mydata' using PigStorage('\t') as (wkt);<br>
 * 		B = foreach A generate WKTGeometry(wkt);<br>
 * @author Rodrigo Ferreira
 *
 *TODO: Support tiles
 */
public class WKTGeometry extends EvalFunc<Tuple> {

	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a WKT geometry
     * @exception java.io.IOException
     * @return wkt geometry as tuple (geometry, data, properties)
     */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {
			
			String wkt = DataType.toString(input.get(0));
			
			Geometry geometry = _geometryParser.parseGeometry(wkt);
			
			Map<String, Object> properties = new HashMap<String, Object>();
			
			/*Computes object id as a hash*/		    	        
		    properties.put("IIUUID",new UUID(null).random());
		    
		    properties.put("class","None");
		    		    
		    Map<String, String> data = new HashMap<String, String>();	//empty data map
		    
		    Tuple tuple = TupleFactory.getInstance().newTuple(3);
		    tuple.set(0, new DataByteArray(new WKBWriter().write(geometry)));
		    tuple.set(1, data);
		    tuple.set(2, properties);
			
		    return tuple;
		    
		} catch (Exception e) {
				throw new IOException("Caught exception processing input row ", e);
		}
		
	}
		
	@Override
    public Schema outputSchema(Schema input) {

		try {
		
			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			list.add(new Schema.FieldSchema("geometry", DataType.BYTEARRAY));
			list.add(new Schema.FieldSchema("data", DataType.MAP));
			list.add(new Schema.FieldSchema("properties", DataType.MAP));
						
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			return new Schema(ts);
			
		} catch (Exception e) {
			return null;
		}
		
    }
	
}
