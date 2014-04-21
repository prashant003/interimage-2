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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.Tile;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A UDF that tests whether a geometry intersects a list of ROIs.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom, tile);<br>
 * 		B = filter A by SpatialFilter(geom,tile);<br>
 * @author Rodrigo Ferreira
 *
 */
public class SpatialFilter extends EvalFunc<Boolean> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	private STRtree _gridIndex = null;
	private STRtree _roiIndex = null;
	private List<Integer> _gridIds = null;
	
	String _roiUrl = null;
	String _gridUrl = null;
	
	/**
	 * Constructor that takes the ROIs and the tiles grid URLs.
	 * */
	public SpatialFilter(String roiUrl, String gridUrl) {
		_roiUrl = roiUrl;
		_gridUrl = gridUrl;
	}
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple; first column is assumed to have a geometry; second column is assumed to have the tile id
     * @exception java.io.IOException
     * @return boolean value
     * 
     * TODO: Use distributed cache; check if an index for the ROIs is necessary
     */
	@SuppressWarnings("unchecked")
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
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
	                InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
			        BufferedReader buff = new BufferedReader(inStream);
				    	                
	    	        ObjectMapper mapper = new ObjectMapper();
	    			List<Tile> tiles = new ArrayList<Tile>();	    			
	    			
	    	        JsonFactory f = new JsonFactory();
		  			JsonParser jp = f.createJsonParser(buff);
		  			
		  			jp.nextToken();
		  			
		  			while (jp.nextToken() == JsonToken.START_OBJECT) {
		  				Tile tile = mapper.readValue(jp, Tile.class);
		  				tiles.add(tile);		  			
		  			}
	    	        
    				for (int i=0; i<tiles.size(); i++) {
    					Geometry geometry = new WKTReader().read(tiles.get(i).getGeometry());
    					_gridIndex.insert(geometry.getEnvelopeInternal(),tiles.get(i).getId());
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

			Object objGeometry = input.get(0);
			Integer tileId = DataType.toInteger(input.get(1));
			
	    	if ((!_roiUrl.isEmpty()) && (!_gridUrl.isEmpty())) {
		        if (_gridIds.contains(tileId)) {		        	
		        	Geometry geometry = _geometryParser.parseGeometry(objGeometry);
	
	        		List<Geometry> list = _roiIndex.query(geometry.getEnvelopeInternal());
	        	
		        	for (Geometry geom : list) {
		        		if (geom.intersects(geometry)) {
		        			return true;
		        		}
		        	}
		        				        	
		        }
		        return false;
	    	} else {
	    		return true;
	    	}
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
