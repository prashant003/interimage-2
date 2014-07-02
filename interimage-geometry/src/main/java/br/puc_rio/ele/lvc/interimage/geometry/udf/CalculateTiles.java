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
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKTReader;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.Tile;

/**
 * A UDF that computes the tiles a geometry intersects.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom, data, props);<br>
 * 		B = foreach A generate geom, data, ToProps(CalculateTiles(geom), 'tile', props) as props;
 * @author Rodrigo Ferreira
 */
public class CalculateTiles extends EvalFunc<String> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	private STRtree _gridIndex = null;
	private String _gridUrl = null;
	private String _assignment = null;
	
	/**Constructor that takes the tiles grid URL and assignment type.*/
	public CalculateTiles(String gridUrl, String assignment) {		
		_gridUrl = gridUrl;		
		_assignment = assignment;
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry
     * @exception java.io.IOException
     * @return string with intersecting tiles
     */
	@SuppressWarnings("unchecked")
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
     		
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
			
			Object objGeometry = input.get(0);
			Geometry geom = _geometryParser.parseGeometry(objGeometry);
			
			List<Tile> list = _gridIndex.query(geom.getEnvelopeInternal());
						
			List<String> tiles = new ArrayList<String>();
			
			for (Tile t : list) {
				
				Geometry g = _geometryParser.parseGeometry(t.getGeometry());
				
				if (g.intersects(geom)) {
					tiles.add(t.getCode());
				}
				
			}
			
			String tileString = new String();
			
			if (_assignment.equals("multiple")) {
				
				/*Assign to all intersecting tiles*/
				boolean first = true;
				for (String i : tiles) {
					if (first) {
						tileString = i;
						first = false;
					} else {
						tileString = tileString + "," + i;
					}
				}
				
			} else {
				
				/*Assign to the tile with minimum ID*/
				long min = Long.MAX_VALUE;
				
				for (String t : tiles) {
					long id = Long.parseLong(t.substring(1));
					if (id < min)
						min = id;
				}
				
				//TODO: store the full list in a property
				tileString = "T" + String.valueOf(min);
				
			}
			
			return tileString;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
	
}
