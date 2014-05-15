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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
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

import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.Tile;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A UDF that clips geometries in relation to a list of ROIs.<br>
 * For efficiency reasons, it should always be used after SpatialFilter.<br><br>
 * 
 * Some observations:<br>
 * 1 - For the geometries that do not intersect any ROI a null bag will be returned
 * 2 - The geometries that intersect more than one ROI will produce the respective number of tuples
 * 3 - Makes no sense to call this UDF after SpatialFilter when it is used with the 'containment' filter type
 * <br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom, tile);<br>
 * 		B = filter A by SpatialFilter(geom,tile,'intersection');
 * 		C = foreach B generate flatten(SpatialClip(B));<br>
 * @author Rodrigo Ferreira
 *
 */
public class SpatialClip extends EvalFunc<DataBag> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	private STRtree _gridIndex = null;
	private STRtree _roiIndex = null;
	private List<Integer> _gridIds = null;
	
	String _roiUrl = null;
	String _gridUrl = null;
	
	/**Constructor that takes the ROIs and the tiles grid URLs.*/
	public SpatialClip(String roiUrl, String gridUrl) {
		_roiUrl = roiUrl;
		_gridUrl = gridUrl;
	}
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple<br>
     * first column is assumed to have a tuple
     * @exception java.io.IOException
     * @return a bag with tuples with the clipped geometries, or a null bag in case of no intersection
     * 
     * TODO: Use distributed cache; check if an index for the ROIs is necessary; deal with data
     */
	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		//executes initialization
		if (_gridIndex == null) {
			_gridIndex = new STRtree();
			_roiIndex = new STRtree();
			_gridIds = new ArrayList<Integer>();
			
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
    					_gridIndex.insert(geometry.getEnvelopeInternal(),t.getId());
				    }
			        			        
	        	}
	        } catch (Exception e) {
				throw new IOException("Caught exception reading grid file ", e);
			}
	        
	        //Creates index for the ROIs
	        //Also creates a list with the Ids of the tiles that intersect the ROIs
	        try {
	        	
	        	if (!_roiUrl.isEmpty()) {
	        			        		
	        		URL url  = new URL(_roiUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
	                InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
			        BufferedReader buff = new BufferedReader(inStream);
			        
			        String line;
			        while ((line = buff.readLine()) != null) {
			        	Geometry geometry = new WKTReader().read(line);
			        	_roiIndex.insert(geometry.getEnvelopeInternal(),geometry);
			        	_gridIds.addAll(_gridIndex.query(geometry.getEnvelopeInternal()));
			        }

	        	}
	        } catch (Exception e) {
				throw new IOException("Caught exception reading ROI file ", e);
			}
	        
		}
		
		try {

			Tuple tuple = DataType.toTuple(input.get(0));
			
			Object objGeometry = tuple.get(0);
			Map<String,String> data = (Map<String,String>)tuple.get(1);
			Map<String,Object> properties = DataType.toMap(tuple.get(2));
			
			Integer tileId = (Integer)properties.get("tile");
						
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
	    	if ((!_roiUrl.isEmpty()) && (!_gridUrl.isEmpty())) {
		        if (_gridIds.contains(tileId)) {
		        	Geometry geometry = _geometryParser.parseGeometry(objGeometry);
	
	        		List<Geometry> list = _roiIndex.query(geometry.getEnvelopeInternal());
	  	        		
		        	for (Geometry geom : list) {

		        		if (geom.intersects(geometry)) {
		        			Geometry g = geom.intersection(geometry);
		        			
		        			byte[] bytes = new WKBWriter().write(g);
		        			
		        			Tuple t = TupleFactory.getInstance().newTuple(3);
		        			t.set(0,new DataByteArray(bytes));
		        			t.set(1,data);
		        			t.set(2,properties);
		        			bag.add(t);
		        				        
		        		    properties.put("IIUUID",new UUID(null).random());
		        			
		        		}
		        		
		        	}
		        			        				        	
		        }
		        
	    	} else {
	    		bag.add(tuple);	    		
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
