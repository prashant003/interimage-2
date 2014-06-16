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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.SpatialIndex;
import br.puc_rio.ele.lvc.interimage.geometry.Tile;

/**
 * A UDF that recalculates the tiles according to a neighborhood criterion.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geometry, data, properties);<br>
 * 		B = foreach A generate geom, data, ToProps(CalculateTiles(geometry), 'tile', props) as props;<br>
 * 		C = foreach B flatten(Replicate(geometry, data, properties)) as (geometry, data, properties);<br>
 * 		D = group C by properties#'tile';<br>
 * 		E = foreach D generate flatten(ReplicateNeighborhood(C)) as (geometry, data, properties);
 * @author Rodrigo Ferreira
 *
 */
public class ReplicateNeighborhood extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	private STRtree _gridIndex = null;
	private String _gridUrl = null;
	@SuppressWarnings("unused")
	private Double _distance = null;
	
	/**Constructor that takes the tiles grid URL and the distance used to compute the neighborhood.*/
	public ReplicateNeighborhood(String gridUrl, String distance) {		
		_gridUrl = gridUrl;		
		if (!distance.isEmpty())
			_distance = Double.parseDouble(distance);		
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
	
	private boolean isReplicated(String iiuuid, String tileId, Map<String,List<String>> replicated) {
		if (replicated.containsKey(iiuuid)) {
			List<String> list = replicated.get(iiuuid);
			return list.contains(tileId);				
		} else {
			return false;
		}
	}
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a bag
     * @exception java.io.IOException
     * @return a bag with the neighborhood assignment
     */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() == 0)
            return null;
	
		Map<String,List<String>> replicated = new HashMap<String,List<String>>();
		
		//executes initialization
		if (_gridIndex == null) {
			_gridIndex = new STRtree();
			
			//Creates an index for the grid
	        try {
	        	
	        	if (!_gridUrl.isEmpty()) {
	        		
	        		URL url  = new URL(_gridUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
			        InputStream buff = new BufferedInputStream(urlConn.getInputStream());				    	    	        
			        ObjectInputStream in = new ObjectInputStream(buff);
	    			
	    		    List<Tile> tiles = (List<Tile>)in.readObject();
	    		    
	    		    in.close();
				    
				    for (Tile t : tiles) {
				    	Geometry geometry = new WKTReader().read(t.getGeometry());
    					_gridIndex.insert(geometry.getEnvelopeInternal(),t);
				    }
			        			        
	        	}
	        } catch (Exception e) {
				throw new IOException("Caught exception reading grid file ", e);
			}
	       	        
		}
		
		try {
			
			DataBag bag1 = DataType.toBag(input.get(0));
						
			SpatialIndex index = createIndex(bag1);
						
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
			Iterator it = bag1.iterator();
			
	        while (it.hasNext()) {
	        	
	            Tuple t1 = (Tuple)it.next();
	            
	            Geometry geom1 = _geometryParser.parseGeometry(t1.get(0));
	            	            
	            Map<String,Object> props1 = DataType.toMap(t1.get(2));
	            
	            String tileId = DataType.toString(props1.get("tile"));
	            
	            /*Computing tiles*/
	            List<Tile> tiles = _gridIndex.query(geom1.getEnvelopeInternal());
	            
	            Geometry tileGeom = null;
	            
	            /*Get tile geometry*/
	            for (Tile t : tiles) {
	            	if (t.getCode().equals(tileId)) {
	            		tileGeom = _geometryParser.parseGeometry(t.getGeometry());
	            	}
	            }
	            
	            /*Check if it is a boundary polygon*/
	            if (!geom1.within(tileGeom)) {
	            	            	
	            	/*Compute the neighboring polygons*/
		        	List<Tuple> list = index.query(geom1.getEnvelopeInternal());
		        	
		        	for (Tuple t2 : list) {
		        				        		
	        			Geometry geom2 = _geometryParser.parseGeometry(t2.get(0));
	        			Map<String,String> data = (Map<String,String>)t2.get(1);
	        			Map<String,Object> props2 = DataType.toMap(t2.get(2));	        			
	        			
	        			String iiuuid = DataType.toString(props2.get("iiuuid"));
	        			
	        			/*If it's in fact a neighboring polygon*/
	        			if (geom2.intersects(geom1)) {
	        					        			
		        			/*If the neighboring polygon is not a boundary polygon*/		        			
		        			if (geom2.within(tileGeom)) {
		        				
		        				List<String> neighboringTiles = new ArrayList<String>();
		        				
			        			for (Tile i : tiles) {
			        				
			        				Geometry tg = _geometryParser.parseGeometry(i.getGeometry());
			        				
			        				/*If it's in fact an intersecting tile*/
			        				if (geom1.intersects(tg)) {
			        				
				        				/*if the tile is not the current one*/
				        				if (!i.getCode().equals(tileId)) {
				        											        			
				        					//if (!isReplicated(iiuuid, i.getCode(), replicated)) {
				        					
				        						neighboringTiles.add(i.getCode());
				        											        				
						        				/*if (replicated.containsKey(iiuuid)) {
						        					List<String> l = replicated.get(iiuuid);
						        					l.add(i.getCode());
						        				} else {
						        					List<String> l = new ArrayList<String>();
							        				l.add(i.getCode());
							        				replicated.put(iiuuid, l);
						        				}*/
						        				
				        					//}
					        				
				        				}
				        				
			        				}
			        				
			        			}
				        				        				
		        				/*Replicates for the tile with the lowest ID*/
		        				long min = Long.MAX_VALUE;
		        				long tid = Long.parseLong(tileId.substring(1));
		        				
		        				for (String t : neighboringTiles) {
		        					long id = Long.parseLong(t.substring(1));
		        					if (id < min)
		        						min = id;
		        				}
		        				
		        				String tileString = "T" + String.valueOf(min);
		        				
		        				/* Avoids that one polygon is replicated to same tile more than once.
		        				 * Also replicates only for tiles with lower IDs than the current one.
		        				 * */		        				
		        				if ((!isReplicated(iiuuid, tileString, replicated)) && (min<tid)) {
		        				
			        				byte[] bytes = new WKBWriter().write(geom2);
			        				
			        				Map<String,Object> newProps = new HashMap<String,Object>(props2);
			        				
			        				newProps.put("tile", tileString);
			        				newProps.put("iinrep", "true");
			        				
			        				Tuple newTuple = TupleFactory.getInstance().newTuple(3);
			        				newTuple.set(0,new DataByteArray(bytes));
			        				newTuple.set(1,new HashMap<String,String>(data));
			        				newTuple.set(2,newProps);
			        				bag.add(newTuple);
				        			
			        				if (replicated.containsKey(iiuuid)) {
			        					List<String> l = replicated.get(iiuuid);
			        					l.add(tileString);
			        				} else {
			        					List<String> l = new ArrayList<String>();
				        				l.add(tileString);
				        				replicated.put(iiuuid, l);
			        				}
	        				
		        				}
		        				
		        			}
		        			
	        			}
	            		
		        	}
		        	
		        	/*Adding original boundary polygon*/
		        	bag.add(t1);
		        	
	            } else {
	            	/*Adding original internal polygon*/
    				bag.add(t1);
	            }
	            
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
