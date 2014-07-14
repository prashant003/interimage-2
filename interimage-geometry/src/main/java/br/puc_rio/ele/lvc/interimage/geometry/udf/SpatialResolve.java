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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.SpatialIndex;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that combines several bags into one considering spatial overlaps and resolving them.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geometry, data, properties);<br>
 * 		B = load 'mydata2' as (geometry, data, properties);<br>
 * 		C = cogroup A by properties#'tile', B by properties#'tile';<br>
 * 		D = flatten(SpatialResolve(A,B));
 * @author Rodrigo Ferreira
 * 
 */
public class SpatialResolve extends EvalFunc<DataBag> {
		
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**This method computes a spatial resolve using the index nested loop method.*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void computeIndexNestedLoopSpatialResolve(DataBag bag1, List<DataBag> bagList, DataBag output) {
		
		try {
			
			int size = bagList.size();
			
			SpatialIndex[] index = new SpatialIndex[size];
			
			Iterator it = bag1.iterator();
	        while (it.hasNext()) {
	            Tuple t1 = (Tuple)it.next();
	        	Geometry geom1 = null;
	            
	        	geom1 = _geometryParser.parseGeometry(t1.get(0));
	        	
	        	Map<String,Object> props1 = DataType.toMap(t1.get(2));
				
				double membership1 = DataType.toDouble(props1.get("membership"));
					
				for (int k=0; k<size; k++)
					index[k] = createIndex(bagList.get(k));
				
	        	List<Tuple> list = new ArrayList<Tuple>();
	        	
	        	for (int k=0; k<size; k++) {
	        		List<Tuple> l = index[k].query(geom1.getEnvelopeInternal());
	        		list.addAll(l);
	        	}
	        	
	        	for (Tuple t2 : list) {
	        		
	        		Geometry geom2 = _geometryParser.parseGeometry(t2.get(0));
        		
	        		Map<String,Object> props2 = DataType.toMap(t2.get(2));
		        	
		        	double membership2 = DataType.toDouble(props2.get("membership"));
	        		
	        		if (geom1.intersects(geom2)) {
	        			
	        			if (membership1 >= membership2) {
	        				Geometry g = geom2.difference(geom1);
	        				byte[] bytes = new WKBWriter().write(g);
	        				t2.set(0, new DataByteArray(bytes));
	        			} else {
	        				Geometry g = geom1.difference(geom2);
	        				byte[] bytes = new WKBWriter().write(g);
	        				t1.set(0, new DataByteArray(bytes));
	        			}
	        			
	        		}
	        		
	        	}
	        	
	        	output.add(t1);
	        	
	        }
	        
		} catch (Exception e) {
			System.err.println("Failed to compute the spatial resolve; error - " + e.getMessage());
		}

	}
	
	/**This method creates an STR-Tree index for the input bag and returns it.*/
	@SuppressWarnings("rawtypes")
	private SpatialIndex createIndex(DataBag bag) {
		
		SpatialIndex index = null;
		
		try {
		
			index = new SpatialIndex();
			
			Iterator it = bag.iterator();
	        while (it.hasNext()) {
	            Tuple t = (Tuple)it.next();
            	Geometry geometry = _geometryParser.parseGeometry(t.get(0));
				index.insert(geometry.getEnvelopeInternal(),t);				
	        }
		} catch (Exception e) {
			System.err.println("Failed to index bag; error - " + e.getMessage());
			return null;
		}
		
		return index;
	}
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * the columns are assumed to have the bags
     * @exception java.io.IOException
     * @return a bag with the input bags spatially resolved
     */
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
		
		try {
						
			DataBag output = BagFactory.getInstance().newDefaultBag();
						
			for (int i=0; i<input.size(); i++) {
				DataBag bag1 = DataType.toBag(input.get(i));				
				
				if (i<(input.size()-1)) {//not necessary for the last bag
				
					List<DataBag> bagList = new ArrayList<DataBag>();
					
					for (int j=i+1; j<input.size(); j++) {
						
						DataBag bag2 = DataType.toBag(input.get(i));
						bagList.add(bag2);
						
					}
					
					computeIndexNestedLoopSpatialResolve(bag1, bagList, output);
					
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
