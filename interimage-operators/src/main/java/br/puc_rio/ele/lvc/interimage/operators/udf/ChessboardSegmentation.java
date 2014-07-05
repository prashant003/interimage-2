package br.puc_rio.ele.lvc.interimage.operators.udf;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

public class ChessboardSegmentation extends EvalFunc<DataBag> {

	//private final GeometryParser _geometryParser = new GeometryParser();
	private Double _segmentSize;
	private String _imageUrl;
	private String _image;
	
	public ChessboardSegmentation(String imageUrl, String image, String segmentSize) {
		_segmentSize = Double.parseDouble(segmentSize);
		_imageUrl = imageUrl;
		_image = image;
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry<br>
     * second column is assumed to have the data<br>
     * third column is assumed to have the properties
     * @exception java.io.IOException
     * @return a bag with the polygons created by the segmentation
     */
	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {
						
			//Object objGeometry = input.get(0);
			Map<String,String> data = (Map<String,String>)input.get(1);
			Map<String,Object> properties = DataType.toMap(input.get(2));
			
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
			//Geometry geometry = _geometryParser.parseGeometry(objGeometry);
			
			String tileStr = DataType.toString(properties.get("tile"));
			
			//double box[] = new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()};
	        
	        if (br.puc_rio.ele.lvc.interimage.common.URL.exists(_imageUrl + _image + "_" + tileStr + ".tif")) {	//if tile doesn't exist
				
	        	/*Getting width and height*/
	        	URL worldFile1 = new URL(_imageUrl + _image + "_" + tileStr + ".meta");
				URLConnection urlConn1 = worldFile1.openConnection();
                urlConn1.connect();
				InputStreamReader inStream1 = new InputStreamReader(urlConn1.getInputStream());
		        BufferedReader reader1 = new BufferedReader(inStream1);
		        
		        double[] imageTileGeoBox = new double[4];
		        
		        String line1;
		        int index1 = 0;
		        while ((line1 = reader1.readLine()) != null) {
		        	if (!line1.trim().isEmpty()) {
		        		if (index1==3)
		        			imageTileGeoBox[0] = Double.parseDouble(line1);
		        		else if (index1==4)
		        			imageTileGeoBox[1] = Double.parseDouble(line1);
		        		else if (index1==5)
		        			imageTileGeoBox[2] = Double.parseDouble(line1);
		        		else if (index1==6)
		        			imageTileGeoBox[3] = Double.parseDouble(line1);
			        	index1++;
		        	}
		        }
						        
				/*Computing segments*/
		        int numTilesX = (int)Math.ceil((imageTileGeoBox[2]-imageTileGeoBox[0]) / _segmentSize);
		        int numTilesY = (int)Math.ceil((imageTileGeoBox[3]-imageTileGeoBox[1]) / _segmentSize);
		        
		        for (int j=0; j<numTilesY; j++) {
		        	for (int i=0; i<numTilesX; i++) {
		        		        		
		        		Tuple t = TupleFactory.getInstance().newTuple(3);
		        				        		
		        		double geoX = i*_segmentSize + imageTileGeoBox[0];
						double geoY = j*_segmentSize + imageTileGeoBox[1];
						
						Geometry geom = new WKTReader().read(String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, Math.min(geoX + _segmentSize, imageTileGeoBox[2]), geoY, Math.min(geoX + _segmentSize, imageTileGeoBox[2]), Math.min(geoY + _segmentSize, imageTileGeoBox[3]), geoX, Math.min(geoY + _segmentSize, imageTileGeoBox[3]), geoX, geoY));
		        		
		        		byte[] bytes = new WKBWriter().write(geom);
		        		
		        		t.set(0,new DataByteArray(bytes));
		        		t.set(1,new HashMap<String,String>(data));
		        		t.set(2,new HashMap<String,Object>(properties));
		        		bag.add(t);
		        		
		        	}
		        }
				
			} else {
				throw new Exception("Could not retrieve image information.");
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
