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
import java.util.Date;
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

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.Tile;
import br.puc_rio.ele.lvc.interimage.common.UUID;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A UDF that computes the union of a bag of geometries.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom, props);<br>
 * 		B = group A by props#'class';<br>
 * 		C = foreach B generate flatten(spatialunion(A));
 * @author Rodrigo Ferreira
 *
 */
public class SpatialUnion extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	
	private STRtree _gridIndex = null;
	private String _gridUrl = null;
	
	public SpatialUnion(String gridUrl) {
		_gridUrl = gridUrl;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
		
		System.out.println(new Date().toString());
		
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
		
		DataBag bag1 = DataType.toBag(input.get(0));
		
		System.out.println(bag1.size());
		
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		
		Geometry union = null;
		
		String className = null;
		
		Iterator it = bag1.iterator();
        while (it.hasNext()) {
            Tuple t1 = (Tuple)it.next();
            Geometry geometry = _geometryParser.parseGeometry(t1.get(0));
                        
            if (className == null) {
            	Map<String,Object> props = DataType.toMap(t1.get(2));
            	className = (String)props.get("class");
            }
            
            List<Tile> list = _gridIndex.query(geometry.getEnvelopeInternal());
						
			boolean within = false;
			
			for (Tile t : list) {
				
				Geometry g = _geometryParser.parseGeometry(t.getGeometry());
				
				if (g.contains(geometry)) {
					within = true;
					break;
				}
				
			}
            
			if (within) {
				Tuple tuple = TupleFactory.getInstance().newTuple(3);
	        	
	        	tuple.set(0,t1.get(0));
    			tuple.set(1,t1.get(1));
    			tuple.set(2,t1.get(2));
				
				bag.add(tuple);
			} else {
				if (union == null) {
					union = geometry;
				} else {
					union = union.union(geometry);
				}
			}
			
        }
		
        for (int k=0; k<union.getNumGeometries(); k++) {
        
	        Tuple tuple = TupleFactory.getInstance().newTuple(3);
	    	
	        byte[] bytes = new WKBWriter().write(union.getGeometryN(k));
	        
	        Map<String,String> data = new HashMap<String,String>();
			data.put("0", "");
	        		
			Map<String,Object> props = new HashMap<String,Object>(); 
			
			String id = new UUID(null).random();
			
			props.put("tile", "");
			props.put("iiuuid", id);
			props.put("class", className);
			props.put("membership", "");
			props.put("crs", "");
			
	    	tuple.set(0,new DataByteArray(bytes));
			tuple.set(1,data);
			tuple.set(2,props);
			
			bag.add(tuple);
			
        }
       
        System.out.println(new Date().toString());
        
		return bag;
		
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
