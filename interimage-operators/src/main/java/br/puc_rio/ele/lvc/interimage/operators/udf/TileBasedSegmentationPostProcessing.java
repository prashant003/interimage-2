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

package br.puc_rio.ele.lvc.interimage.operators.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.SpatialIndex;

/**
 * UDF for post processing tile based segmentation, i.e. reducing the artifacts among tile borders.
 * @author Patrick Happ, Rodrigo Ferreira
 *
 */

//TODO: Use the adjacent tile with the major intersection area?

public class TileBasedSegmentationPostProcessing extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	private String _imageUrl = null;
	private String _image = null;
	private double _eucThreshold = 0.0;
	private final double BufDist = 1.1; //Constant for spatial buffer distance
	private int _numBands = 0;
	private long _groupTileId = 0;
	
	
	/**Constructor that takes the tiles grid URL*/
	public TileBasedSegmentationPostProcessing(String imageUrl, String image, String eucThreshold) {		
		_imageUrl = imageUrl;
		_image = image;
		_eucThreshold = Double.parseDouble(eucThreshold);
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
		
		try {
			
			DataBag bag1 = DataType.toBag(input.get(0));
			SpatialIndex index = createIndex(bag1);
			Iterator it = bag1.iterator();
			
			WKTWriter writer = new WKTWriter();
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
			List<String> mergedList = new ArrayList<String>();
			
	        while (it.hasNext()) {
	        	
	            Tuple t1 = (Tuple)it.next();
	            
	            Geometry geom1 = _geometryParser.parseGeometry(t1.get(0));
	            //Map<String,String> data = (Map<String,String>)t1.get(1);
	            Map<String,Object> props1 = DataType.toMap(t1.get(2));
	            
	            String tileId = DataType.toString(props1.get("tile"));
	            long myId = Long.parseLong(tileId.substring(1));
	            
	            //Only process polygons in the current tile (TileId = GroupId)
	            if (_groupTileId == myId){ //current tile
					//Get Mean from polygon
		        	String name="Mean_";
		        	
		        	double[] mean = new double[_numBands];
		        	for (int i=0; i < _numBands;i++)
		        		mean[i] = DataType.toDouble(props1.get(name.concat(String.valueOf(i))));
		        	
		        	//search for neighbors with bounding box
					List<Tuple> list = index.query(geom1.buffer(BufDist).getEnvelopeInternal());
		        	for (Tuple t2 : list) {
		        		Geometry geom2 = _geometryParser.parseGeometry(t2.get(0));
		        		
		        		//If it's in fact a neighboring polygon
	        			if (geom2.intersects(geom1.buffer(BufDist))) {		        		

	        				Map<String,Object> props2 = DataType.toMap(t2.get(2));
	        				long nbId = Long.parseLong(DataType.toString(props2.get("tile")).substring(1));
	
	        				//check only neighbors from another tile
	        				if (nbId != myId){ //TODO: test this. This test should be irrelevant.
	        					//Get iiuuid
	        					String iiuidNb = DataType.toString(props2.get("iiuuid"));
	        					
	        					//Process only those that have not been merged
	        					if (!mergedList.contains(iiuidNb)){
		        					
		        					//Get Mean from neighbor and calculate the Euclidean distance 
		    			        	double[] meanNB = new double[_numBands];
		    			        	double dist = 0;
		    			        	
		    			        	for (int i=0; i< _numBands; i++){
		    			        		meanNB[i] = DataType.toDouble(props2.get(name.concat(String.valueOf(i))));
		    			        		dist = ((mean[i]-meanNB[i]) * (mean[i]-meanNB[i])) + dist;
					        		}
		    			        	dist = Math.sqrt(dist);
		    			        	
		    			        	System.out.println("Analysing");
	    			        		System.out.println(DataType.toString(props1.get("iiuuid")));
	    			        		System.out.println(iiuidNb);
		    			        	
		    			        	//If the value is under Threshold then merge polygons
		    			        	if (dist <= _eucThreshold){
		    			        		//update mean
		    			        		double a1 = geom1.getArea();
		    			        		double a2 = geom2.getArea();
		    			        		for (int i=0; i< _numBands; i++){
		    			        			mean[i] = (mean[i]*a1 + meanNB[i]*a2) / (a1+a2);
		    			        			props1.put(name.concat(String.valueOf(i)),mean[i]);
		    			        		}
		    			        		//merge polygons
		    			        		geom1 = geom1.union(geom2);
		    			        		mergedList.add(iiuidNb);
		    			        		
		    			        		System.out.println("MERGED!!");
		    			        	}
	        					} 		
	        				}
	        			}
        			}
		        	//update tuple
		        	t1.set(0, writer.write(geom1));
	        		t1.set(2, props1);//TODO: check if it is necessary	
	        		
	        		//write the polygons from the tile (modified or not)
					bag.add(t1);  
	            }  	        		
			}
	        
	        //process the objects of another tile in order to write to the bag
	        Iterator it2 = index.itemsTree().iterator();
	        IndexRecursiveIterator(it2, bag, mergedList);
	        
			return bag;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
		
	}
	
	
	/**This method iterate over an STR-tree*/
	//TODO: This method works, but need a double check
	private static void IndexRecursiveIterator(Iterator it, DataBag bag, List<String> mergedList ) throws ExecException{
		while (it.hasNext()) {
        	Object obj= it.next();
        	Tuple t = TupleFactory.getInstance().newTuple(1);
        	if (obj.getClass() == t.getClass()){
        		t = null;
        		System.out.println("Tuple");
        		Tuple t3 = (Tuple)obj;
        		String iiuuid = DataType.toString(DataType.toMap(t3.get(2)).get("iiuuid"));
        		//Not merged
        		if (!mergedList.contains(iiuuid)){
            		bag.add(t3); 
        		}
        	} else {
        		IndexRecursiveIterator(((List) obj).iterator(), bag, mergedList);
        		System.out.println("recursive");
        	}
        }
	}
	
	/**This method creates an STR-Tree index for the input bag and returns it.*/
	@SuppressWarnings("rawtypes")
	private SpatialIndex createIndex(DataBag bag) {
		
		SpatialIndex index = null;
		
		try {
		
			index = new SpatialIndex();
					
			Iterator it = bag.iterator();
			
			String groupId=null;
			//Process the first tuple
			if (it.hasNext()){
				Tuple t = (Tuple)it.next();
				//get information
				groupId = DataType.toString(DataType.toMap(t.get(2)).get("GroupID"));
				_groupTileId = Long.parseLong(groupId.substring(1));
				_numBands = GetNumBands(groupId);
				
				//only objects from another tile
	        	if (!DataType.toString(DataType.toMap(t.get(2)).get("tile")).equals(groupId)){
	        		Geometry geometry = _geometryParser.parseGeometry(t.get(0));
	        		index.insert(geometry.getEnvelopeInternal(),t);
	        	}
			}
			
			//Process the rest
	        while (it.hasNext()) {
	        	Tuple t = (Tuple)it.next();
	        	//only objects from another tile
	        	if (!DataType.toString(DataType.toMap(t.get(2)).get("tile")).equals(groupId)){
	        		Geometry geometry = _geometryParser.parseGeometry(t.get(0));
	        		index.insert(geometry.getEnvelopeInternal(),t);
	        	}
	        }
		} catch (Exception e) {
			System.err.println("Failed to index bag; error - " + e.getMessage());
			return null;
		}
				
		return index;
	}
	
	
	/**This method reads metadata to get the number of bands
	 * @throws Exception */
	private int GetNumBands(String tileStr) throws Exception {
    	URL worldFile1 = new URL(_imageUrl + _image + "/" + tileStr + ".meta");
		URLConnection urlConn1 = worldFile1.openConnection();
        urlConn1.connect();
		InputStreamReader inStream1 = new InputStreamReader(urlConn1.getInputStream());
        BufferedReader reader1 = new BufferedReader(inStream1);
                
        String line1 = reader1.readLine();
        
        reader1.close();
        inStream1.close();
        
        return Integer.parseInt(line1);
	}
	
	
	
	@Override
    public Schema outputSchema(Schema input) {
        
		try {

			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
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
