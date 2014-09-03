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
import java.awt.image.WritableRaster;
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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.operators.Pixel;
import br.puc_rio.ele.lvc.interimage.operators.Segment;
import br.puc_rio.ele.lvc.interimage.data.Image;
import br.puc_rio.ele.lvc.interimage.data.imageioimpl.plugins.tiff.TIFFImageReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.io.WKTWriter;

//TODO: This could be a generic UDF that receives the parameters and compute a particular segmentation process.
//TODO: Create an interface for segmentation and then each implementation

/**
 * UDF for mutual Baatz segmentation.
 * @author Patrick Happ, Rodrigo Ferreira
 */

public class MutualMultiresolutionSegmentation extends EvalFunc<DataBag> {
	//Initializing threshold with default value
	//private static double THRESHOLD=0.02;
	
	//private final GeometryParser _geometryParser = new GeometryParser();
	//private Double _segmentSize;
	private String _imageUrl;
	private String _image;
	//private STRtree _roiIndex = null;
	//private String _roiUrl = null;
	
	static ArrayList <Segment> _segmentsPtr;
	static int [] _visitingOrder;
	
	private static int _nbands;
	private static int _imageH;
	private static int _imageW;
	
	private static double _scale; //this is scale^2!!
	private static double _wColor;
	private static double _wCmpt;
	private static double [] _wBand;
	
	public MutualMultiresolutionSegmentation (String imageUrl, String image, String scale, String wColor, String wCmpt, String wBands) {
		//_segmentSize = Double.parseDouble(segmentSize);
		_imageUrl = imageUrl;
		_image = image;
		//_roiUrl = roiUrl;
		
		_scale = Math.pow(Double.parseDouble(scale),2);
		_wColor = Double.parseDouble(wColor);
		_wCmpt = Double.parseDouble(wCmpt);
		
		_segmentsPtr = null;
		
		_nbands=0;
		_imageH=0;
		_imageW=0;
		
		/*Reading and normalizing band weights*/
		String[] bands = wBands.split(",");

		double sum = 0.0;
		
		_wBand = new double[bands.length];
		for (int i=0; i<bands.length; i++) {
			sum = sum + Double.parseDouble(bands[i]);
		}
		
		for (int i=0; i<bands.length; i++) {
			_wBand[i] = Double.parseDouble(bands[i]) / sum;
		}
		
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
		/*if (_roiIndex == null) {
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
	        
		}*/
		
		try {
			
			//Object objGeometry = input.get(0);
			Map<String,String> data = (Map<String,String>)input.get(1);
			Map<String,Object> properties = DataType.toMap(input.get(2));
			
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			//Geometry inputGeometry = _geometryParser.parseGeometry(objGeometry);
			String tileStr = DataType.toString(properties.get("tile"));
			String inputURL = _imageUrl + _image + "/" + tileStr + ".tif";
			
			//double box[] = new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()};
	        //if (br.puc_rio.ele.lvc.interimage.common.URL.exists(inputURL)) {	//if tile doesn't exist (???)
				
				_segmentsPtr = new ArrayList <Segment>();
	        	
				//read image and create seeds
	        	initializeSegments(inputURL);
				
				randomOrder();
								
	        	//iterates over the segments and do the region growing
	        	computeSegmentation();	        	
	        		        	
	        	//Write Results
	        	
	        	//Get Geocoordinates
	        	URL worldFile1 = new URL(_imageUrl + _image + "/" + tileStr + ".meta");
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
		        
		        //double resX = (imageTileGeoBox[2]-imageTileGeoBox[0])/_imageW;
		        /*Using a threshold that represents one tenth of the resolution*/
		        //THRESHOLD = resX / 10;
		        
			    //ArrayList< ArrayList<double[]> > rings = new ArrayList< ArrayList<double[]> >();
			    //double CoordX, CoordY, CoordX2, CoordY2;
		        
		        int idPix;
		        GeometryFactory fact = new GeometryFactory();
		        WKTWriter writer = new WKTWriter();
		        UUID uuid = new UUID(null);
		        		
		        List<Geometry> list = new ArrayList<Geometry>();
		        
			    for (Segment aux_segment : _segmentsPtr) {
			    	 //int [] outterRing = {0};
			    	 //rings.clear();
 
			    	Pixel auxPixel = aux_segment.getPixel_list();
			    	
			    	if (auxPixel != null){
			    					    	
				    	while (auxPixel != null) {
	
				    		double CoordX, CoordY;
			      				
				    		idPix=auxPixel.getId();
							int x = idPix % _imageW;
							int y = idPix / _imageW;
							 
		      				Coordinate [] linePoints = new Coordinate[5];
							//left top corner
		      				CoordX = Image.imgToGeoX(x - 0.5, _imageW, imageTileGeoBox);
		      				CoordY = Image.imgToGeoY(y - 0.5 ,_imageH, imageTileGeoBox);
		      				linePoints[0]= new Coordinate(CoordX,CoordY);
		      				linePoints[4]= new Coordinate(CoordX,CoordY); //close the ring
			                //right top corner
		      				CoordX = Image.imgToGeoX(x + 0.5, _imageW,imageTileGeoBox);
		      				CoordY = Image.imgToGeoY(y - 0.5,_imageH,imageTileGeoBox);
		      				linePoints[1] = new Coordinate(CoordX,CoordY);
							//right bottom corner
							CoordX = Image.imgToGeoX(x + 0.5,_imageW,imageTileGeoBox);
							CoordY = Image.imgToGeoY(y + 0.5,_imageH,imageTileGeoBox);
							linePoints[2] = new Coordinate(CoordX,CoordY);
							//right bottom corner
							CoordX = Image.imgToGeoX(x - 0.5,_imageW,imageTileGeoBox);
							CoordY = Image.imgToGeoY(y + 0.5,_imageH,imageTileGeoBox);
							linePoints[3] = new Coordinate(CoordX,CoordY);
							 
							LinearRing shell = fact.createLinearRing(linePoints);		          			
		          			//Geometry poly = new Polygon(shell, null, fact);
							Geometry poly = fact.createPolygon(shell, null);
							 
		          			list.add(poly);
		          			
		          			auxPixel = auxPixel.getNext_pixel();
		          			
				        }
		        		
			        	Geometry[] geoms = new Geometry[list.size()];
			        	
			        	int index = 0;
			        	for (Geometry geom : list) {
			        		geoms[index] = geom;
			        		index++;
			        	}
			        	
			        	//TODO: union seems slightly faster than buffer(0)
			        	//TODO: Passing the factory as a parameter here created a huge performance issue
			        	//For now, calling the method from the factory, much faster
			        	//Geometry union = new GeometryCollection(geoms, fact).buffer(0);
			        	Geometry union = fact.createGeometryCollection(geoms).buffer(0);
			        	
						Tuple t = TupleFactory.getInstance().newTuple(3);
					
		        		//byte[] bytes = writer.write(union);
		        								
		        		//String compressed = GeometryParser.compressGeometryToString(union);
		        		
		        		Map<String,Object> props = new HashMap<String,Object>(properties);
		        		
		        		String id = uuid.random();
		        		
		        		props.put("iiuuid", id);
		        		
		        		t.set(0,writer.write(union));
		        		t.set(1,new HashMap<String,String>(data));
		        		t.set(2,props);
		        		bag.add(t);
		        	
		        		list.clear();
		        		
			    	}
					 
			    }
			    _segmentsPtr.clear();
		        
			return bag;
			
		} catch (Exception e) {
			e.printStackTrace();
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
			
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			Schema bagSchema = new Schema(ts);
			
			Schema.FieldSchema bs = new Schema.FieldSchema(null, bagSchema, DataType.BAG);
			
			return new Schema(bs);

		} catch (Exception e) {
			return null;
		}
		
    }
	
	private static void initializeSegments(String imageFile) throws Exception{	
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
		
        ImageReadParam param = reader.getDefaultReadParam();       
        BufferedImage buff = reader.read(0,param);
        
		if (buff == null)
			throw new Exception("Could not instantiate tile image: " + imageFile.toString());
        
		WritableRaster raster = buff.getRaster();
		
        _imageH=reader.getHeight(0);
        _imageW=reader.getWidth(0);

        _nbands=raster.getNumBands();
        
        if (_nbands != _wBand.length){
        	throw new Exception("Image bands are incompatible with band weights parameter");
        }
        
        Segment auxSegment;
		Pixel bodyPixel;
        int idPixel=0;
        //for each line
        for (int row = 0; row < _imageH; row++) {          	          	
          	//for each pixel
          	for (int col=0; col<_imageW; col++){
          		
	          	//Create a segment for the pixel
          		auxSegment = Segment.create(idPixel);
          		_segmentsPtr.add(auxSegment);
          		
          		
          	//auxSegment.id = idPixel;
				auxSegment.setB_boxByIndex((double)row, 0);
				auxSegment.setB_boxByIndex((double)col, 1);

				auxSegment.createSpectral(_nbands);
				double val;
				for(int b = 0; b < _nbands; b++)
				{
					val = raster.getSampleDouble(col, row, b);
					auxSegment.setAvg_colorByIndex(val, b);
					auxSegment.setStd_colorByIndex(0, b);
					auxSegment.setAvg_color_squareByIndex(val*val, b);
					auxSegment.setColor_sumByIndex(val, b);
				}
								
				//auxSegment.pixelList.put(idPixel, true);
				
				bodyPixel = Pixel.create(idPixel);
				auxSegment.setPixel_list(bodyPixel);
				auxSegment.setLast_pixel(bodyPixel);
				bodyPixel.setBorderline(true);
				bodyPixel.setNext_pixel(null);

				idPixel++;
          	}
      }
        
      stream.close();
	}

	private static void computeSegmentation(){
		Segment currSegment = null;
		Segment currNeighbor = null;
		Pixel auxPixel = null;
		
		ArrayList <Integer> nbSeg= new ArrayList <Integer>();
		
		boolean hasMerged=true;
		//int step=0;
		while (hasMerged){
			hasMerged=false;
			
			for (int i=0; i<_visitingOrder.length; i++){
				currSegment = _segmentsPtr.get( _visitingOrder[i]);
								
				if (!currSegment.isUsed()){
					currSegment.setUsed(true);
					auxPixel = currSegment.getPixel_list();

					/* for each outline pixel */
					double minFusion;
					while (auxPixel!=null)
					{
						 int id=auxPixel.getId();
						 if (auxPixel.isBorderline()){
							int [] nb = getPixelIdFromNeighbors(id);
							  
							for (int n=0;n<4;n++)
							{
								if (nb[n] != -1)
								{
									nb[n] = _segmentsPtr.get(nb[n]).getId();
									//if neighbor is not from same segment
									if (nb[n]!= i){
										if (!nbSeg.contains(nb[n])){
											nbSeg.add(nb[n]);
										}
									}
									
								}
								
							}
						 }
						//next pixel
						 auxPixel = auxPixel.getNext_pixel();
					}
					
					/* calculate heterogeinity factors for each neighbor */
					minFusion =  Double.MAX_VALUE;
					double spectral, spatial;
					double fusion;
					int bestNbId = -1;
					
					for (int n: nbSeg){
						currNeighbor = _segmentsPtr.get(n);
						
						spectral=0; spatial=0;
						if (_wColor > 0){
							spectral = calcColorStats(currSegment, currNeighbor) * _wColor;	
						}
									
						if (spectral < _scale){ //already the square
							//only if spatial is wanted
							if ((1- _wColor) > 0)
							{
								// calculates spatial heterogeneity
								spatial = CalcSpatialStats(currSegment, currNeighbor) * (1-(_wColor));
							}
							
							//fusion factor
							fusion = spectral + spatial;			
							if (fusion < _scale){ //already the square
								if (fusion < minFusion){
									minFusion = fusion;
									bestNbId = n;
								}
								//always merge with lower ID (when it is a draw)
								else if (fusion == minFusion && bestNbId < n){
									bestNbId = n;
								}
							}
						}					 
					}
					if (bestNbId != -1){
						currNeighbor = _segmentsPtr.get(bestNbId);
						mergeSegment(currSegment, currNeighbor);
						hasMerged=true;
					}
					nbSeg.clear();
				}
			}
			resetSegments();
			//step++;
			//System.out.println(step);
		}
		
	}

	public static void randomOrder() throws Exception
	{
		//TODO: Put it on random order
		_visitingOrder= new int[_imageW * _imageH];
		for (int i=0; i< _visitingOrder.length; i++){
			_visitingOrder[i]=i;
		}
	}
	
	public static int[] getPixelIdFromNeighbors (int pixelId){    
	    int x = pixelId % _imageW;
		int y = pixelId / _imageW;
		
		int [] neighb =new int[4];
		for (int i=0; i<4; i++){
			neighb[i]=-1;
		}
		
		if (y>0) 
			neighb[0]=(y-1)* _imageW + x; //north
		if (x>0) 
			neighb[1]= y * _imageW + (x-1);//west
		if (y < _imageH-1) 
			neighb[2]=(y+1)* _imageW + x; //south
		if (x < _imageW-1) 
			neighb[3]= y* _imageW + (x+1);//east
		
		return neighb;
	}
	
	public static double calcColorStats (Segment obj, Segment neighb)
	{
		double[] mean = new double[_nbands];
		double[] colorSum = new double[_nbands];
		double[] squarePixels = new double[_nbands];
		double[] stddev = new double[_nbands];
		
		double[] color_f = new double [_nbands];
		double color_h=0;
		
		double areaObj, areaNb, areaRes;
		
		areaObj = obj.getArea();
		areaNb =  neighb.getArea();
		areaRes = areaObj+areaNb;
		
		// calculates color factor per band and total
		color_h = 0;
		for (int b = 0; b < _nbands; b++)
		{
			mean[b] = ((obj.getAvg_colorByIndex(b) *areaObj)+(neighb.getAvg_colorByIndex(b)*areaNb))/areaRes;
			squarePixels[b] = (obj.getAvg_color_squareByIndex(b))+(neighb.getAvg_color_squareByIndex(b));	
			colorSum[b] = obj.getColor_sumByIndex(b) + neighb.getColor_sumByIndex(b);	
			stddev[b] = Math.sqrt(Math.abs(squarePixels[b] - 2*mean[b]*colorSum[b] + areaRes*mean[b]*mean[b])/areaRes);
						
			color_f[b] = _wBand[b] * ((areaRes*stddev[b]) - ((areaObj* obj.getStd_colorByIndex(b)) + (areaNb * neighb.getStd_colorByIndex(b))));
			color_h += color_f[b];
		}
		
		return color_h;
	}
	
	public static double CalcSpatialStats (Segment obj, Segment neighb){
		double areaObj, areaNb, areaRes;
		double spatial_h =0, smooth_f=0, compact_f =0;
		double perimObj, perimNb, perimRes;
		double bboxObjLen, bboxNbLen, bboxResLen;
		double [] bboxRes;
		
		areaObj = obj.getArea();
		areaNb =  neighb.getArea();
		areaRes = areaObj+areaNb;
		
		perimObj = obj.getPerimeter();
		perimNb = neighb.getPerimeter();
		
		if (areaRes<4){ /* valid only if pixel neighborhood==4 */
			if (areaRes ==2){
				perimRes=6;
			}
			else {
				perimRes=8;
			}
		}
		else{
			perimRes = calcPerimeter(obj, neighb);
		}
		
		bboxObjLen = obj.getB_boxByIndex(2)*2 +  obj.getB_boxByIndex(3)*2;
		bboxNbLen =  neighb.getB_boxByIndex(2)*2 +  neighb.getB_boxByIndex(3)*2;
		bboxRes = calcBbox(obj, neighb);		
		bboxResLen = bboxRes[2]*2 + bboxRes[3]*2;
		
		/* smoothness factor */
		smooth_f = (areaRes*perimRes/bboxResLen - 
		 (areaObj*perimObj/bboxObjLen + areaNb*perimNb/bboxNbLen));

		/* compactness factor */
		compact_f = (Math.sqrt(areaRes)*perimRes - 
		 (Math.sqrt(areaObj)*perimObj + Math.sqrt(areaNb)*perimNb));

		/* spatial heterogeneity */
		spatial_h = _wCmpt*compact_f + (1-_wCmpt)*smooth_f;


		return spatial_h;
	}
	
	public static double calcPerimeter (Segment obj, Segment neighb){
		double perimTotal;
		int idNb;
		Pixel auxPixel;
		
		perimTotal = obj.getPerimeter() + neighb.getPerimeter();
		//choose segment with smaller perimeter
		if ( obj.getPerimeter() <= neighb.getPerimeter() )
		{
			auxPixel = obj.getPixel_list();
			idNb = neighb.getId();
		}
		else
		{
			auxPixel = neighb.getPixel_list();
			idNb = obj.getId();
		}
		
		// for each pixel from the smaller perimeter segment
		while ( auxPixel != null){
			// just for borderline
			if ( auxPixel.isBorderline() )
			{
				int [] nb = getPixelIdFromNeighbors(auxPixel.getId());

				 //verify the neighbor pixels
				 for (int i = 0; i < 4; i++ )
				 {
					   if ( nb[i] != -1 ) // if it is not image limit 
					   {
						   nb[i] = _segmentsPtr.get(nb[i]).getId();
						   if (idNb == nb[i]){ //if it is part of the bigger
							   perimTotal =-2;  
						   }
					   }
				 }
			}
			auxPixel = auxPixel.getNext_pixel();
		}
		return perimTotal;
	}
	
	public static double[] calcBbox (Segment obj, Segment neighb){
			double [] bbox=new double[4];
			double minRowObj, maxRowObj, minColObj, maxColObj;
			double minRowNb, maxRowNb, minColNb, maxColNb;
			double minRowRes, maxRowRes, minColRes, maxColRes;
		 		   
			minRowObj = obj.getB_boxByIndex(0);
			maxRowObj = obj.getB_boxByIndex(0)+obj.getB_boxByIndex(2);
			minColObj = obj.getB_boxByIndex(1);
			maxColObj = obj.getB_boxByIndex(1)+obj.getB_boxByIndex(3);
			
			minRowNb = neighb.getB_boxByIndex(0);
			maxRowNb = neighb.getB_boxByIndex(0)+neighb.getB_boxByIndex(2);
			minColNb = neighb.getB_boxByIndex(1);
			maxColNb = neighb.getB_boxByIndex(1)+neighb.getB_boxByIndex(3);
		 
		   if (minRowObj < minRowNb) minRowRes=minRowObj; else minRowRes=minRowNb;
		   if (minColObj < minColNb) minColRes=minColObj; else minColRes=minColNb;
		   if (maxRowObj > maxRowNb) maxRowRes=maxRowObj; else maxRowRes=maxRowNb;
		   if (maxColObj > maxColNb) maxColRes=maxColObj; else maxColRes=maxColNb;
		 
		   bbox[0] = minRowRes;
		   bbox[1] = minColRes;
		   bbox[2] = maxRowRes-minRowRes;
		   bbox[3] = maxColRes-minColRes;
		   
		   return bbox;
	 }
	
	private static void mergeSegment(Segment obj, Segment neighb){	
		double areaObj, areaNb, areaRes;
		
		neighb.setUsed(true);
		
		areaObj = obj.getArea();
		areaNb =  neighb.getArea();
		areaRes = areaObj+areaNb;
		
		// calculates color factor per band and total
		for (int b = 0; b < _nbands; b++)
		{
			obj.setAvg_colorByIndex(((obj.getAvg_colorByIndex(b) *areaObj)+(neighb.getAvg_colorByIndex(b)*areaNb))/areaRes,b);
			obj.setAvg_color_squareByIndex((obj.getAvg_color_squareByIndex(b))+(neighb.getAvg_color_squareByIndex(b)),b);	
			obj.setColor_sumByIndex((obj.getColor_sumByIndex(b))+(neighb.getColor_sumByIndex(b)),b);	
			obj.setStd_colorByIndex(Math.sqrt(Math.abs(obj.getAvg_color_squareByIndex(b) 
					- 2*obj.getAvg_colorByIndex(b)*obj.getColor_sumByIndex(b) 
					+ areaRes*obj.getAvg_colorByIndex(b)*obj.getAvg_colorByIndex(b))/areaRes),b);
		}
	

		obj.setPerimeter(calcPerimeter(obj, neighb));
		obj.setB_box(calcBbox(obj, neighb));
		
		obj.setArea(areaRes);
		
		resetPixels(obj, neighb);
	}
	
	
	private static void resetPixels(Segment obj, Segment neighb){	
		int  neighborId, segmentId;
		Pixel auxPixel;
	
		neighborId = neighb.getId();
		segmentId = obj.getId();
	
		/* for each pixel of the neighbor segment to be merged */
		auxPixel = neighb.getPixel_list();
		while (auxPixel !=null)
		{
			/* changes the value of the pixel in the segment matrix (assign it to the current segment) */
			_segmentsPtr.get( auxPixel.getId()).setId(segmentId);
	
			 /* if it is outline pixel, check if that must be changed */
			 if (auxPixel.isBorderline()==true) /* curr_segment->area>6, only valid for pixel neighborhood==4 */
			 {
				 int [] nb = getPixelIdFromNeighbors(auxPixel.getId());
				 /* if pixel is surrounded by pixels of the same segment or of the merged neighbor, 
					it's no inter a border pixel */
				 int i=0;
				 while ((i<4)&&(i>-1)){
					 if (nb[i] == -1){ /* image limit */
						 i=-1; 
					 } else {
						 nb[i] = _segmentsPtr.get(nb[i]).getId();
						 if ((nb[i] != segmentId)&&(nb[i] != neighborId))
					     {
					    	 i=-1;
					     }
					     else
					     {
					    	 i++;
					     }
					 }
				 }
				 if (i!=-1) /* no inter outline pixel */
				 {
					 auxPixel.setBorderline(false);
				 }
			 }
			 auxPixel = auxPixel.getNext_pixel();
		}
	
		if (obj.getArea()>6) /* curr_segment->area>6, only valid for pixel neighborhood==4 */
		{
			 /* for each outline pixel of the current segment */
			auxPixel = obj.getPixel_list();
			 while (auxPixel != null)
			 {
			   if (auxPixel.isBorderline()==true)
			   {
				   int [] nb = getPixelIdFromNeighbors(auxPixel.getId());
					 /* if pixel is surrounded by pixels of the same segment or of the merged neighbor, 
						it's no longer a border pixel */
					 int i=0;
					 while ((i<4)&&(i>-1)){
						 if (nb[i] == -1){ /* image limit */
							 i=-1; 
						 } else {
							 nb[i] = _segmentsPtr.get(nb[i]).getId();
							 if ((nb[i] != segmentId)&&(nb[i] != neighborId))
						     {
						    	 i=-1;
						     }
						     else
						     {
						    	 i++;
						     }
						 }
					     
					 }
					 if (i!=-1) /* no longer outline pixel */
					 {
						 auxPixel.setBorderline(false);
					 }
				 }
				 auxPixel = auxPixel.getNext_pixel();
			 }
		}
	
		/* include pixel list of neighbor in the list of curr_segment */
		obj.getLast_pixel().setNext_pixel(neighb.getPixel_list());
		obj.setLast_pixel(neighb.getLast_pixel());
		neighb.setPixel_list(null);
		
	}

	private static void resetSegments(){	
		int  id;
		Segment aux_segment;

		/* mark all segments as unused */
		for (int i=0; i< _visitingOrder.length; i++)
		{
			id = _visitingOrder[i];
			aux_segment = _segmentsPtr.get(id);
			if (aux_segment.getId() != id){
				//TODO: Remove segment from VisitngOrder
				//i--;
			} else {
				aux_segment.setUsed(false);
			}
			
			
		}
	}
	
}
