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

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.stream.ImageInputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.operators.LimiarSegment;
import br.puc_rio.ele.lvc.interimage.data.imageioimpl.plugins.tiff.TIFFImageReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;


//TODO: This could be a generic UDF that receives the parameters and compute a particular segmentation process.
//TODO: Create an interface for segmentation and then each implementation

public class Limiarization extends EvalFunc<DataBag> {
	//TODO: Verify this THRESHOLD value
	final static double THRESHOLD=0.02;
	
	//private final GeometryParser _geometryParser = new GeometryParser();
	//private Double _segmentSize;
	private String _imageUrl;
	private String _image;
	private STRtree _roiIndex = null;
	private String _roiUrl = null;
	
	private static int _nbands;
	private static int _imageH;
	private static int _imageW;
		
	private static double []  _threshold;
	private static String [] _class; 
	private static Operation _operation;
	private static int []  _bandsOperation;

	private static HashMap<Integer, LimiarSegment> _segmentList;
	public enum Operation {EXPRESSION, BRIGHTNESS, INDEX}
	
	double [] _imageTileGeoBox;
	
	public Limiarization (String imageUrl, String image, String roiUrl, String thresholds, String classes, String operation) throws Exception {
		//_segmentSize = Double.parseDouble(segmentSize);
		_imageUrl = imageUrl;
		_image = image;
		_roiUrl = roiUrl;
		
		//thresholds
		String[] tr = thresholds.split(",");
		_threshold = new double [tr.length];
		for (int i=0; i< tr.length; i++) {
			if (tr[i].equals("-inf") ){
				_threshold[i]=Double.MIN_VALUE;
			} else if (tr[i].equals("inf")){
				_threshold[i]=Double.MAX_VALUE;
			} else{
				_threshold[i] = Double.parseDouble(tr[i]);
			}
		}
		//TODO: verify if threshold is in order. It will a problem if it is not! 
		
		//classes
		String[] cl = classes.split(",");
		_class= new String[cl.length];
		if (cl.length == (tr.length-1)){
			for (int i=0; i< cl.length; i++) {
				_class[i]= cl[i];
			}
		} else{
			throw new Exception("Problem with input thresholds and classes");
		}
		
		//operation
		if (operation.contains("Expression")){
			_operation= Operation.EXPRESSION;
			//TODO: Parse expression
			_bandsOperation=null;
		} else if (operation.contains("Index")){
			_operation= Operation.INDEX;
			try{
				String[] bands = operation.substring(operation.indexOf("(")+1, operation.indexOf(")")).split(",");
				_bandsOperation=new int[2];
				_bandsOperation[0] = Integer.parseInt(bands[0]);
				_bandsOperation[1] = Integer.parseInt(bands[1]);
			}catch (Exception e){
				throw new Exception("Problem with input operation");
			}
		}else if (operation.contains("Brightness")){
			_operation= Operation.BRIGHTNESS;
			_bandsOperation=null;
		} else {
			throw new Exception("Problem with input operation");
		}
		
		
		_segmentList = null;
		
		_nbands=0;
		_imageH=0;
		_imageW=0;	
		_imageTileGeoBox=null;
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
        
		//executes initialization
		if (_roiIndex == null) {
			_roiIndex = new STRtree();
						        
	        //Creates index for the ROIs
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
			        }

	        	}
	        } catch (Exception e) {
				throw new IOException("Caught exception reading ROI file ", e);
			}
	        
		}
		
		try {
						
			
			//Object objGeometry = input.get(0);
			Map<String,String> data = (Map<String,String>)input.get(1);
			Map<String,Object> properties = DataType.toMap(input.get(2));
			
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			//Geometry geometry = _geometryParser.parseGeometry(objGeometry);
			String tileStr = DataType.toString(properties.get("tile"));
			String inputURL = _imageUrl + _image + "_" + tileStr + ".tif";
			
			//double box[] = new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()};
	        if (br.puc_rio.ele.lvc.interimage.common.URL.exists(inputURL)) {	//if tile doesn't exist (???)
				        	
	        	//Get Geocoordinates
	        	URL worldFile1 = new URL(_imageUrl + _image + "_" + tileStr + ".meta");
				URLConnection urlConn1 = worldFile1.openConnection();
                urlConn1.connect();
				InputStreamReader inStream1 = new InputStreamReader(urlConn1.getInputStream());
		        BufferedReader reader1 = new BufferedReader(inStream1);
		        
		        _imageTileGeoBox = new double[4];
		        
		        String line1;
		        int index1 = 0;
		        while ((line1 = reader1.readLine()) != null) {
		        	if (!line1.trim().isEmpty()) {
		        		if (index1==3)
		        			_imageTileGeoBox[0] = Double.parseDouble(line1);
		        		else if (index1==4)
		        			_imageTileGeoBox[1] = Double.parseDouble(line1);
		        		else if (index1==5)
		        			_imageTileGeoBox[2] = Double.parseDouble(line1);
		        		else if (index1==6)
		        			_imageTileGeoBox[3] = Double.parseDouble(line1);
			        	index1++;
		        	}
		        }
		        
	        	_segmentList = new HashMap<Integer, LimiarSegment>();
		        
		        try {
					thresholding(inputURL);
					//initialize_segment_neihgborhood();
				} catch (Exception e) {
					throw new Exception("Problem with segmentation");
				}
		        
		        
		        //Here we have segments with their polygons!
		        //TODO: Merge Polygons and write the output
		        
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
	
	private static void thresholding(String imageFile) throws Exception{	
		//Read Image
		URL worldFile1 = new URL(imageFile);
		URLConnection urlConn = worldFile1.openConnection();
        urlConn.connect();
        
        InputStream stream = new BufferedInputStream(urlConn.getInputStream());
        ImageInputStream in = ImageIO.createImageInputStream(stream);
        
        if (in == null)
        	throw new Exception("Could not create input stream: " + imageFile.toString());
        
        TIFFImageReader reader = new TIFFImageReader(null);
        reader.setInput(in);
        
        //TODO: Optimize this
        ImageReadParam param = reader.getDefaultReadParam();       
        BufferedImage buff = reader.read(0,param);
        
		if (buff == null)
			throw new Exception("Could not instantiate tile image: " + imageFile.toString());
        
        _imageH=reader.getHeight(0);
        _imageW=reader.getWidth(0);

        _nbands=buff.getRaster().getNumBands();

        if (_operation != Operation.BRIGHTNESS){
        	if (_bandsOperation.length > _nbands){
        		throw new Exception("Image bands are incompatible with input bands operation");
        	}
        }

        int id=0;
        //for each line
        for (int y = 0; y < _imageH; y++) {          	          	
          	//for each pixel
          	for (int x=0; x<_imageW; x++){
          		
          		double val=0;
          		//check the operation
          		switch(_operation){
          			case BRIGHTNESS:
          				int sum=0;
          				for(int b = 0; b < _nbands; b++) {
          					sum += buff.getRaster().getSample(x, y, b);	
        		        }
          				val = (double) sum/_nbands;
          				break;
          			case INDEX:
          				int idx1 = buff.getRaster().getSample(x, y, _bandsOperation[0]);
          				int idx2 = buff.getRaster().getSample(x, y, _bandsOperation[1]);
          				
          				val = (double) (idx1 - idx2)/ (idx1 + idx2);
          				break;
          			case EXPRESSION:
          				int [] bandVal =  new int [_bandsOperation.length];
          				for(int b = 0; b < _bandsOperation.length; b++) {
          					bandVal[b] = buff.getRaster().getSample(x, y, _bandsOperation[b]);
          					//TODO: IMPLEMENT ARITHMETIC EXPRESSION
          					val =0;
        		        }
          				break;
          			default:
          				throw new Exception("operation not found");
          		}
          		
           		//generate segment only if it has a valid value
          		if ((_threshold[0] <= val) && (val <= _threshold[_threshold.length-1])){
          			//class is in the last range by default
          			int classId = _class.length -1;
          			
          			//test if it is in a different range
          			for (int i=1; i < _threshold.length - 1; i++){
              			if (val < _threshold[i]){
              				classId = i-1;
              				break;
              			}
              		}
          			
          			LimiarSegment seg = new LimiarSegment(id, _class[classId]);
          			_segmentList.put(id, seg);
          			    
      				double []imgGeo = {0.0, (double)_imageH, (double)_imageW, 0.0};
      				double CoordX, CoordY;
      				
      				Coordinate [] linePoints = new Coordinate[5];
					//left top corner
      				CoordX = pic2geoX(x - 0.5, _imageW, imgGeo);
      				CoordY = pic2geoY(y - 0.5 ,_imageH, imgGeo);
      				linePoints[0]= new Coordinate(CoordX,CoordY);
      				linePoints[4]= new Coordinate(CoordX,CoordY); //close the ring
	                //right top corner
      				CoordX = pic2geoX(x + 0.5, _imageW,imgGeo);
      				CoordY = pic2geoY(y - 0.5,_imageH,imgGeo);
      				linePoints[1] = new Coordinate(CoordX,CoordY);
					//left bottom corner
					CoordX = pic2geoX(x - 0.5,_imageW,imgGeo);
					CoordY = pic2geoY(y + 0.5,_imageH,imgGeo);
					linePoints[2] = new Coordinate(CoordX,CoordY);
					//right bottom corner
					CoordX = pic2geoX(x + 0.5,_imageW,imgGeo);
					CoordY = pic2geoY(y + 0.5,_imageH,imgGeo);
					linePoints[3] = new Coordinate(CoordX,CoordY);
					 
					LinearRing shell = new GeometryFactory().createLinearRing(linePoints);
          			GeometryFactory fact = new GeometryFactory();
          			Polygon poly = new Polygon(shell, null, fact);
          			
          			seg.setPolygon(poly);          			
          		}
          		
          		
          		id++;
          	}
      }
        
                
      stream.close(); 
	}
	
	//TODO: The following methods can be used from a geometry class
	private static double pic2geoX(double picX, int cols, double [] imgGeo)
	{
	    return ((picX+0.5) * ((imgGeo[2]-imgGeo[0]) / cols)) + imgGeo[0];
	}
	
	private static double pic2geoY(double picY, int rows, double [] imgGeo)
	{
		return ((picY+0.5) * ((imgGeo[1]-imgGeo[3]) / rows)) + imgGeo[3];
	}
}
