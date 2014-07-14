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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.SpatialIndex;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that spatially groups geometries.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom);<br>
 * 		B = load 'mydata2' as (geom);<br>
 * 		C = SpatialGroup(A,B,2);<br>
 * @author Rodrigo Ferreira
 * <br><br>
 * The method provided by this class is described in this paper:
 * Edwin H. Jacox and Hanan Samet. 2007. Spatial join techniques.
 * ACM Trans. Database Syst. 32, 1, Article 7 (March 2007).
 * DOI=10.1145/1206049.1206056 http://doi.acm.org/10.1145/1206049.1206056
 * 
 * TODO: Should create new objects as in clip?
 *   
 */
public class SpatialGroup extends EvalFunc<DataBag> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	private Double _distance = null;
	
	/**Constructor that takes the distance used to group the objects.*/
	public SpatialGroup(String distance) {
		if (!distance.isEmpty())
			_distance = Double.parseDouble(distance);		
	}
	
	/**This method computes a spatial grouping using the index nested loop method.*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void computeIndexNestedLoopGroup(DataBag bag1, List<DataBag> bagList, DataBag output) {
		
		try {
			
			int size = bagList.size();
			
			SpatialIndex[] index = new SpatialIndex[size];
			
			Iterator it = bag1.iterator();
	        while (it.hasNext()) {
	            Tuple t1 = (Tuple)it.next();
	        	Geometry geometry = null;
	            
	        	if (_distance != null) {
	        		geometry = _geometryParser.parseGeometry(t1.get(0)).buffer(_distance);
	        	} else {
	        		geometry = _geometryParser.parseGeometry(t1.get(0));
	        	}
					
				for (int k=0; k<size; k++)
					index[k] = createIndex(bagList.get(k));
					
	        	Tuple tuple1 = TupleFactory.getInstance().newTuple(4);
	        	
	        	List<Tuple> list = new ArrayList<Tuple>();
	        	
	        	for (int k=0; k<size; k++) {
	        		List<Tuple> l = index[k].query(geometry.getEnvelopeInternal());
	        		list.addAll(l);
	        	}
	        	
	        	tuple1.set(0,t1.get(0));
	        	tuple1.set(1,t1.get(1));
	        	tuple1.set(2,t1.get(2));
	        	
	        	DataBag bag = BagFactory.getInstance().newDefaultBag();
	        	
	        	Tuple tuple2 = TupleFactory.getInstance().newTuple(3*size);
	        	
	        	int count = 0;
	        	
	        	for (Tuple t2 : list) {
	        		
        			tuple2.set(count+0,t2.get(0));
        			tuple2.set(count+1,t2.get(1));
        			tuple2.set(count+2,t2.get(2));
        			        			
        			count = count + 3;

	        	}
	        	
	        	bag.add(tuple2);
	        	
	        	tuple1.set(3,bag);
	        	output.add(tuple1);
	        	
	        }
	        
		} catch (Exception e) {
			System.err.println("Failed to compute the grouping; error - " + e.getMessage());
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
     * the columns are assumed to have the bags; the first is the reference and the others are the targets
     * @exception java.io.IOException
     * @return a bag with the reference tuple and the grouped target tuples
     */
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
		
		try {
			
			DataBag output = BagFactory.getInstance().newDefaultBag();
			
			DataBag bag1 = DataType.toBag(input.get(0));
			
			List<DataBag> bagList = new ArrayList<DataBag>();
			
			for (int i=1; i<input.size(); i++) {
				DataBag bag2 = DataType.toBag(input.get(i));
				bagList.add(bag2);				
			}
			
			computeIndexNestedLoopGroup(bag1, bagList, output);
			
			return output;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		
		try {

			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			
			for (int i=0; i<(input.size()-1); i++) {
				list.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
				list.add(new Schema.FieldSchema(null, DataType.MAP));
				list.add(new Schema.FieldSchema(null, DataType.MAP));
			}
			
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			Schema bagSchema = new Schema(ts);
						
			List<Schema.FieldSchema> list2 = new ArrayList<Schema.FieldSchema>();
			list2.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
			list2.add(new Schema.FieldSchema(null, DataType.MAP));
			list2.add(new Schema.FieldSchema(null, DataType.MAP));
			list2.add(new Schema.FieldSchema(null, bagSchema, DataType.BAG));
			
			Schema tupleSchema2 = new Schema(list2);
			
			Schema.FieldSchema ts2 = new Schema.FieldSchema(null, tupleSchema2, DataType.TUPLE);
			
			Schema bagSchema2 = new Schema(ts2);
			
			Schema.FieldSchema bs2 = new Schema.FieldSchema(null, bagSchema2, DataType.BAG);
			
			return new Schema(bs2);

		} catch (Exception e) {
			return null;
		}
		
    }
	
}
