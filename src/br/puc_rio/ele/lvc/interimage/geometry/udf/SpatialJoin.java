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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.SpatialIndex;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.AbstractNode;
import com.vividsolutions.jts.index.strtree.Boundable;
import com.vividsolutions.jts.index.strtree.ItemBoundable;
import com.vividsolutions.jts.geom.Envelope;

/**
 * A UDF that spatially joins geometries.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom,tile);<br>
 * 		B = load 'mydata2' as (geom,tile);<br>
 * 		C = cogroup A by tile, B by tile;<br>
 *		D = filter C by not IsEmpty(A);<br>
 *		E = filter D by not IsEmpty(B);<br>
 * 		F = foreach E generate flatten(SpatialJoin(A,B));<br>
 * @author Rodrigo Ferreira
 * <br><br>
 * The methods provided by this class are described in this paper:
 * Edwin H. Jacox and Hanan Samet. 2007. Spatial join techniques.
 * ACM Trans. Database Syst. 32, 1, Article 7 (March 2007).
 * DOI=10.1145/1206049.1206056 http://doi.acm.org/10.1145/1206049.1206056  
 */
public class SpatialJoin extends EvalFunc<DataBag> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	String _joinType = null;
	
	public SpatialJoin(String joinType) {
		_joinType = joinType;
	}
	
	/**This method computes intersecting pairs using the index nested loop method.<br>
	 * It is used by {@link #computeHierarchicalTraversalJoin(DataBag, DataBag, DataBag, boolean)}.*/
	@SuppressWarnings("unchecked")	
	private List<Map<String,Boundable>> findIntersectingPairs(Map<String,Boundable> map) {
		
		List<Map<String,Boundable>> list = new ArrayList<Map<String,Boundable>>();
		
		try {
		
			Boundable first = map.get("first");
			Boundable second = map.get("second");
			
			if ((first instanceof AbstractNode) && (second instanceof AbstractNode)) {
			
				List<Boundable> l1 = ((AbstractNode)first).getChildBoundables();
				List<Boundable> l2 = ((AbstractNode)second).getChildBoundables();
				
				SpatialIndex index = new SpatialIndex();
				
				if (l1.size() > l2.size()) {
					for (Boundable b1 : l1) {
						index.insert((Envelope)b1.getBounds(),b1);
					}
					
					for (Boundable b2 : l2) {
						List<Boundable> l3 = index.query((Envelope)b2.getBounds());
						for (Boundable b3 : l3) {
							Map<String,Boundable> auxMap = new HashMap<String,Boundable>();
							auxMap.put("first", b3);
							auxMap.put("second", b2);
							list.add(auxMap);
						}
					}
					
				} else {
					for (Boundable b2 : l2) {
						index.insert((Envelope)b2.getBounds(),b2);
					}
					
					for (Boundable b1 : l1) {
						List<Boundable> l3 = index.query((Envelope)b1.getBounds());
						for (Boundable b3 : l3) {
							Map<String,Boundable> auxMap = new HashMap<String,Boundable>();
							auxMap.put("first", b1);
							auxMap.put("second", b3);
							list.add(auxMap);
						}
					}
				}
				
			} else if ((first instanceof AbstractNode) && (second instanceof ItemBoundable)) {
				
				List<Boundable> l = ((AbstractNode)first).getChildBoundables();
				
				SpatialIndex index = new SpatialIndex();
								
				for (Boundable b : l) {
					index.insert((Envelope)b.getBounds(),b);
				}
				
				List<Boundable> l1 = index.query((Envelope)second.getBounds());
				
				for (Boundable b1 : l1) {
					Map<String,Boundable> auxMap = new HashMap<String,Boundable>();
					auxMap.put("first", b1);
					auxMap.put("second", second);
					list.add(auxMap);
				}
				
			} else if ((first instanceof ItemBoundable) && (second instanceof AbstractNode)) {
				
				List<Boundable> l = ((AbstractNode)second).getChildBoundables();
				
				SpatialIndex index = new SpatialIndex();
								
				for (Boundable b : l) {
					index.insert((Envelope)b.getBounds(),b);
				}
				
				List<Boundable> l2 = index.query((Envelope)first.getBounds());
				
				for (Boundable b2 : l2) {
					Map<String,Boundable> auxMap = new HashMap<String,Boundable>();
					auxMap.put("first", first);
					auxMap.put("second", b2);
					list.add(auxMap);
				}
				
			}
						
		} catch (Exception e) {
			System.err.println("Failed to compute the join; error - " + e.getMessage());
		}
	
		return list;
	}
	
	/**This method computes a spatial join using a hierarchical traversal method.*/
	private void computeHierarchicalTraversalJoin(DataBag bag1, DataBag bag2, DataBag output, boolean invert) {
	
		try {
			
			SpatialIndex index1 = createIndex(bag1);
			SpatialIndex index2 = createIndex(bag2);
			
			index1.build();
			index2.build();
			
			Boundable root1 = index1.getRoot();
			Boundable root2 = index2.getRoot();
			
			LinkedList<Map<String,Boundable>> queue = new LinkedList<Map<String,Boundable>>();
			
			Map<String,Boundable> auxMap = new HashMap<String,Boundable>();
			auxMap.put("first", root1);
			auxMap.put("second", root2);
			queue.add(auxMap);
			
			while (!queue.isEmpty()) {
				
				Map<String,Boundable> nodePair = queue.poll();
				
				List<Map<String,Boundable>> rectanglesList = findIntersectingPairs(nodePair);
				
				for (Map<String,Boundable> pair : rectanglesList) {
					
					Boundable b1 = pair.get("first");
					Boundable b2 = pair.get("second");
					
					if ((b1 instanceof AbstractNode) && (b2 instanceof AbstractNode)) {						
						queue.add(pair);
					} else if ((b1 instanceof ItemBoundable) && (b2 instanceof ItemBoundable)) {
												
						Tuple tuple = TupleFactory.getInstance().newTuple(6);
						
						Tuple t1 = (Tuple)((ItemBoundable)b1).getItem();
						Tuple t2 = (Tuple)((ItemBoundable)b2).getItem();
						
						if (!invert) {
		        			tuple.set(0,t1.get(0));
		        			tuple.set(1,t1.get(1));
		        			tuple.set(2,t1.get(2));
		        			
		        			tuple.set(3,t2.get(0));
		        			tuple.set(4,t2.get(1));
		        			tuple.set(5,t2.get(2));
	        			} else {
	        				tuple.set(0,t2.get(0));
		        			tuple.set(1,t2.get(1));
		        			tuple.set(2,t2.get(2));
		        			
		        			tuple.set(3,t1.get(0));
		        			tuple.set(4,t1.get(1));
		        			tuple.set(5,t1.get(2));
	        			}
												
						output.add(tuple);
						
					} else if ((b1 instanceof AbstractNode) && (b2 instanceof ItemBoundable)) {
						queue.add(pair);						
					} else if ((b1 instanceof ItemBoundable) && (b2 instanceof AbstractNode)) {
						queue.add(pair);
					}
				}
			}
			
		} catch (Exception e) {
			System.err.println("Failed to compute the join; error - " + e.getMessage());
		}
		
	}
	
	/**This method computes a spatial join using the index nested loop method.*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void computeIndexNestedLoopJoin(DataBag bag1, DataBag bag2, DataBag output, boolean invert) {
		
		try {
			
			SpatialIndex index = createIndex(bag1);
			
			Iterator it = bag2.iterator();
	        while (it.hasNext()) {
	            Tuple t2 = (Tuple)it.next();
	        	Geometry geometry = _geometryParser.parseGeometry(t2.get(0));
	            
	        	List<Tuple> list = index.query(geometry.getEnvelopeInternal());
	        	
	        	for (Tuple t : list) {
	        			
	        			Tuple tuple = TupleFactory.getInstance().newTuple(6);	        			
	        			
	        			if (!invert) {
		        			tuple.set(0,t.get(0));
		        			tuple.set(1,t.get(1));
		        			tuple.set(2,t.get(2));
		        			
		        			tuple.set(3,t2.get(0));
		        			tuple.set(4,t2.get(1));
		        			tuple.set(5,t2.get(2));
	        			} else {
	        				tuple.set(0,t2.get(0));
		        			tuple.set(1,t2.get(1));
		        			tuple.set(2,t2.get(2));
		        			
		        			tuple.set(3,t.get(0));
		        			tuple.set(4,t.get(1));
		        			tuple.set(5,t.get(2));
	        			}
	        			
	        			output.add(tuple);

	        	}
	        	
	        }
	        
		} catch (Exception e) {
			System.err.println("Failed to compute the join; error - " + e.getMessage());
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
     * @param input tuple; first column is assumed to have the first bag; second column is assumed to have another bag
     * @exception java.io.IOException
     * @return a bag with the joined tuples
     */
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
		
		try {
						
			DataBag output = BagFactory.getInstance().newDefaultBag();
			
			DataBag bag1 = (DataBag)input.get(0);
			DataBag bag2 = (DataBag)input.get(1);
			
			if ((bag1.size() == 0) || (bag2.size() == 0))
				return null;
			
			if (bag1.size() > bag2.size()) {
				if (_joinType.equals("index-nested-loop")) {
					computeIndexNestedLoopJoin(bag1, bag2, output, false);
				} else {
					computeHierarchicalTraversalJoin(bag1, bag2, output, false);	
				}				
			} else {
				if (_joinType.equals("index-nested-loop")) {					
					computeIndexNestedLoopJoin(bag2, bag1, output, true);
				} else {					
					computeHierarchicalTraversalJoin(bag2, bag1, output, true);
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
			list.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
			
			list.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
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
