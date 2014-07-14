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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.operators.Pixel;
import br.puc_rio.ele.lvc.interimage.operators.Segment;
import br.puc_rio.ele.lvc.interimage.data.imageioimpl.plugins.tiff.TIFFImageReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;

//TODO: This could be a generic UDF that receives the parameters and compute a particular segmentation process.
//TODO: Create an interface for segmentation and then each implementation

public class MutualBaatzSegmentation extends EvalFunc<DataBag> {
	//Initializing threshold with default value
	private static double THRESHOLD=0.02;
	
	//private final GeometryParser _geometryParser = new GeometryParser();
	//private Double _segmentSize;
	private String _imageUrl;
	private String _image;
	//private STRtree _roiIndex = null;
	//private String _roiUrl = null;
	
	private static int _nbands;
	private static int _imageH;
	private static int _imageW;
	
	private static double _scale; //this is scale^2!!
	private static double _wColor;
	private static double _wCmpt;
	private static double [] _wBand;
	private static HashMap<Integer, Segment> _segmentList;
	
	public MutualBaatzSegmentation (String imageUrl, String image, String scale, String wColor, String wCmpt, String wBands) {
		//_segmentSize = Double.parseDouble(segmentSize);
		_imageUrl = imageUrl;
		_image = image;
		//_roiUrl = roiUrl;
		
		_scale = Math.pow(Double.parseDouble(scale),2);
		_wColor = Double.parseDouble(wColor);
		_wCmpt = Double.parseDouble(wCmpt);
		
		_segmentList = null;
		
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
			String inputURL = _imageUrl + _image + "_" + tileStr + ".tif";
			
			//double box[] = new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()};
	        if (br.puc_rio.ele.lvc.interimage.common.URL.exists(inputURL)) {	//if tile doesn't exist (???)
				
	        	_segmentList = new HashMap<Integer, Segment>();
	        	
	        	//read image and create seeds
	        	initializeSegments(inputURL);
	        	
	        	//iterates over the segments and do the region growing
	        	computeSegmentation();	        	
	        	
	        	//Write Results
	        	
	        	//Get Geocoordinates
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
		        
		        double resX = (imageTileGeoBox[2]-imageTileGeoBox[0])/_imageW;
		        /*Using a threshold that represents one tenth of the resolution*/
		        THRESHOLD = resX / 10;
		        
			    ArrayList< ArrayList<double[]> > rings = new ArrayList< ArrayList<double[]> >();
			    double CoordX, CoordY, CoordX2, CoordY2;

			    for (Segment aux_segment : _segmentList.values()) {
			    	 int [] outterRing = {0};
			    	 rings.clear();
 
					 for (Pixel aux_pixel : aux_segment.getPixelList().values()) {
			            
						 if (aux_pixel.isBorder()){
							//convert from pixel_id to x, y
							int x = aux_pixel.getX(_imageW);
							int y = aux_pixel.getY(_imageW);
							
							int [] pixelNeighborhood = aux_pixel.getPixelIdFromNeighbors(_imageW, _imageH);
							
							//North
							if (!aux_segment.getPixelList().containsKey(pixelNeighborhood[0])){
								//left top corner
								CoordX = pic2geoX(x - 0.5, _imageW, imageTileGeoBox);
								CoordY = pic2geoY(y - 0.5 ,_imageH, imageTileGeoBox);
				                //right top corner
								CoordX2 = pic2geoX(x + 0.5, _imageW,imageTileGeoBox);
								CoordY2 = CoordY;
								
								checkEdge(rings,outterRing,CoordX,CoordY,CoordX2,CoordY2);
							}
							
							//West
							if (!aux_segment.getPixelList().containsKey(pixelNeighborhood[1])){
								//bottom left corner
								CoordX = pic2geoX(x - 0.5,_imageW,imageTileGeoBox);
								CoordY = pic2geoY(y + 0.5,_imageH,imageTileGeoBox);
				                //top left corner
								CoordX2 = CoordX;
				                CoordY2 = pic2geoY(y - 0.5,_imageH,imageTileGeoBox);
				                
				                checkEdge(rings,outterRing,CoordX,CoordY,CoordX2,CoordY2);
							}
							
							//South
							if (!aux_segment.getPixelList().containsKey(pixelNeighborhood[2])){
								//bottom right corner
								CoordX = pic2geoX(x + 0.5,_imageW,imageTileGeoBox);
								CoordY = pic2geoY(y + 0.5,_imageH,imageTileGeoBox);
				                //bottom left corner
								CoordX2 = pic2geoX(x - 0.5,_imageW,imageTileGeoBox);
				                CoordY2 = CoordY;
				                
				                checkEdge(rings,outterRing,CoordX,CoordY,CoordX2,CoordY2);
							}
							
							//East
							if (!aux_segment.getPixelList().containsKey(pixelNeighborhood[3])){					
								//right top corner
								CoordX = pic2geoX(x + 0.5,_imageW,imageTileGeoBox);
								CoordY = pic2geoY(y - 0.5,_imageH,imageTileGeoBox);
				                //right bottom corner
								CoordX2 = CoordX;
								CoordY2 = pic2geoY(y + 0.5,_imageH,imageTileGeoBox);

								checkEdge(rings,outterRing,CoordX,CoordY,CoordX2,CoordY2);
							}
						 }
			        }

					 //ADD THE POLYGON
					 int index=0;
					 ArrayList <LinearRing> LinearRings = new ArrayList <LinearRing> ();
					 LinearRing shell =  null;
					 
					 for (ArrayList<double[]> ring : rings) {
						 if (ring.isEmpty()){
							 index++;
							 continue;
						 }
						 
						 Coordinate [] linePoints = new Coordinate[ring.size()];
						 for (int i=0; i<ring.size(); i++)
						 {   
							 double [] point  = ring.get(i);
							 Coordinate c = new Coordinate(point[0],point[1]);
							 linePoints[i] = c;
						 }
						 
						 LinearRing linearRing = new GeometryFactory().createLinearRing(linePoints);
						 if (index == outterRing[0]) {
							shell = linearRing;
					     } else {
					    	 LinearRings.add(linearRing);
					     }
						 index++;
					 }
					 
					 Polygon pol;
					 GeometryFactory fact = new GeometryFactory();
					 if (LinearRings.size()<1){
						 pol = new Polygon(shell, null, fact);
					 } else{
						 LinearRing[] holes = new LinearRing[LinearRings.size()];
						 for (int i=0; i< LinearRings.size(); i++)
							 holes[i]=LinearRings.get(i);
						 pol = new Polygon(shell, holes, fact);
					 }
					 
					 Geometry geom = pol;
					 					 
					 /*if (_roiIndex.size()>0) {
							//Clipping according to the ROI
							List<Geometry> list = _roiIndex.query(geom.getEnvelopeInternal());
							
							for (Geometry g : list) {
								if (g.intersects(geom)) {
									
									Geometry geometry = g.intersection(geom);
									
									if (geometry.getNumGeometries()>1) {// if it's a geometry collection
										geometry = FilterGeometryCollection.filter(geometry);	//keeping only polygons
										
										if (geometry.isEmpty())
											continue;
																				
										//geometry = geometry.buffer(0);
									} else if (geometry.getNumGeometries()==1) {
										if (!(geometry instanceof Polygon))
											continue;										
									} //else if (geometry.isEmpty()) {
										//continue;
									//}
									
									for (int k=0; k<geometry.getNumGeometries(); k++) {//separating polygons in different records
										
										Tuple t = TupleFactory.getInstance().newTuple(3);
										
										byte[] bytes = new WKBWriter().write(geometry.getGeometryN(k));
						        		
						        		Map<String,Object> props = new HashMap<String,Object>(properties);
						        		
						        		String id = new UUID(null).random();
						        		props.put("iiuuid", id);
						        		
						        		t.set(0,new DataByteArray(bytes));
						        		t.set(1,new HashMap<String,String>(data));
						        		t.set(2,props);
						        		bag.add(t);
						        		
									}
									
								}
							}
							
						} else {*/
						
							//Clipping according to the input geometry
							//if (inputGeometry.intersects(geom)) {
								
								//geom = inputGeometry.intersection(geom);
								
								Tuple t = TupleFactory.getInstance().newTuple(3);
							
				        		byte[] bytes = new WKBWriter().write(geom);
				        		
				        		Map<String,Object> props = new HashMap<String,Object>(properties);
				        		
				        		String id = new UUID(null).random();
				        		
				        		props.put("iiuuid", id);
				        		
				        		t.set(0,new DataByteArray(bytes));
				        		t.set(1,new HashMap<String,String>(data));
				        		t.set(2,props);
				        		bag.add(t);
								
							//}
			        		
						//}
					 
			    }
		        _segmentList.clear();
		        
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
                
        int id_pixel=0;
        //for each line
        for (int row = 0; row < _imageH; row++) {          	          	
          	//for each pixel
          	for (int col=0; col<_imageW; col++){
          		
	          	//Create a segment for the pixel
	      		Segment aux_seg = new Segment(id_pixel);
	      		aux_seg.setNumBands(_nbands);
		  
	      		aux_seg.setMean_x(col);
	      		aux_seg.setMean_y(row);
	      		aux_seg.setSum_x(col);
	      		aux_seg.setSum_y(row);;
	      		aux_seg.setSquare_x(col*col);
	      		aux_seg.setSquare_y(row*row);
	      		aux_seg.setProduct_xy(col*row);

	      		//aux_seg.setBBoxbyIndex(0,row);
	      		//aux_seg.setBBoxbyIndex(1,col);
	      		//aux_seg.setBBoxbyIndex(2,1);
	      		//aux_seg.setBBoxbyIndex(3,1);
				
				//spectral attributes	
				//for each band
	        	for(int b = 0; b < _nbands; b++) {
	        		//TODO: optimize this
	        		double val = raster.getSampleDouble(col, row, b);
	        		            		
	        		aux_seg.avg_color[b] = val;
	        		aux_seg.std_color[b] = 0;
	        		aux_seg.avg_square_color[b] = val*val;
	        		aux_seg.sum_color[b]= val;
		        }
		
				//segment
				aux_seg.setSegment_id(id_pixel);				
				_segmentList.put(id_pixel, aux_seg);
				
				//saving neighborhood
				int[] ids = new int[4];
				ids = aux_seg.getPixelList().get(id_pixel).getPixelIdFromNeighbors(_imageW, _imageH);
				for (int i=0; i<4; i++){
					if (ids[i] > -1){
						aux_seg.getNeighborIds().add(ids[i]);
					}
				}
																		
				id_pixel++;
          	}
      }
        
      stream.close();
	}

	private static void computeSegmentation(){
		int curr_step=1;
		boolean has_merge=true;
		
		//TODO: Optimize the deletion of segments
		ArrayList <Integer> deleteList =  new ArrayList<Integer>();
		
		//Until there is no merge (can't do it forever)
		while ((curr_step< 500) && (has_merge==true))
		{
			// flag that indicates if there were merges in the step
			//has_merge = false;
			
			//for each segment
			for (Segment aux_segment : _segmentList.values()) {
				//reset best neighbors and fusion factor
				aux_segment.resetBestNeighbor();
				
				//find best neighbors
				find_best_neighbor(aux_segment);
			}
			
			//if any best neighbor was found
			//if (has_merge == true)
			{
				has_merge = false;

				//for entire list
				//Iterator<Entry<Integer, Segment>> it = _segmentList.entrySet().iterator();
			    //while (it.hasNext()) {
				for (Segment aux_segment : _segmentList.values()) {
			    	//Segment aux_segment = it.next().getValue();
					int id = aux_segment.getSegment_id();
					
					//Test mutuality condition
					//Iterator<Entry<Integer, Segment>> it_nb = aux_segment.bestNeighbor.entrySet().iterator();
					for (Segment best : aux_segment.getBestNeighbor().values()) {
					//while (it_nb.hasNext()) {
						//Segment best = it_nb.next().getValue();
						if (best.getBestNeighbor().containsKey(id)){
							
							//prepare to remove segment with smaller area
							Segment stay, dead;
							//boolean canDelete=true;
							if (aux_segment.getArea() <= best.getArea()){
								stay = best;
								dead = aux_segment;
								//canDelete=false;

							}else{
								stay = aux_segment;
								dead = best;
							}

							//merge segments
							mergeBestNeighbors(stay, dead);
							has_merge = true;
							
							//guarantee that will not grow again - one merge for step
							stay.resetBestNeighbor();
							dead.resetBestNeighbor();
							
							//remove segment with smaller area
							
							/*if(canDelete == false)
								it.remove();
							else{
								_segmentList.remove(dead.segment_id);
							}*/
							deleteList.add(dead.getSegment_id());
							
							break;
						}
					}
				}
			}
			//remove from list
			for (int i : deleteList)
				_segmentList.remove(i);
			
			//next step
			curr_step++;
	    }
	}

	
	private static void find_best_neighbor(Segment obj){  

		//TODO: Optimize finding segment neighbors (graph version??)
		obj.resetBestNeighbor();
		
		//TODO: Not optimal, calculating for every segment means double the workload!
		 
		double fusion_f;
		double spectral_h;
		double spectral_f;
		double spatial_h;
		double spatial_f;
		
		//get every segment neighbor
		for (int id : obj.getNeighborIds()){
			Segment neighb = _segmentList.get(id);
			
			spectral_h = calc_color_stats(obj, neighb);
			spectral_f = _wColor * spectral_h;
			
			if (spectral_f < _scale){ //already the square
				spatial_f =0;
				//only if spatial is wanted
				if ((1- _wColor) > 0)
				{
					// calculates spatial heterogeneity
					spatial_h = calc_spatial_stats(obj, neighb);
					spatial_f = (1-(_wColor)) * spatial_h;
				}
				
				//fusion factor
				fusion_f = spectral_f + spatial_f;			
				if (fusion_f < _scale){ //already the square
					if (fusion_f < obj.getFusion_f()){
						obj.resetBestNeighbor();
						obj.getBestNeighbor().put(neighb.getSegment_id(), neighb);
						obj.setFusion_f(fusion_f);
					}
					else if (fusion_f == obj.getFusion_f()){
						obj.getBestNeighbor().put(neighb.getSegment_id(), neighb);
					}
				}
				
			}
		}
	}
	
	
	private static double calc_color_stats (Segment obj, Segment neighb)
	{
		double[] mean = new double[_nbands];
		double[] colorSum = new double[_nbands];
		double[] squarePixels = new double[_nbands];
		double[] stddev = new double[_nbands];
		double[] stddevNew = new double[_nbands];
		
		double[] wband_norm = new double[_nbands];
		double[] color_f = new double [_nbands];
		double color_h;
		
		double areaObj, areaNb, areaRes;
		
		areaObj = obj.getArea();
		areaNb =  neighb.getArea();
		areaRes = areaObj+areaNb;
		
		// calculates color factor per band and total
		color_h = 0;
		for (int b = 0; b < _nbands; b++)
		{
			mean[b] = ((obj.avg_color[b] *areaObj)+(neighb.avg_color[b]*areaNb))/areaRes;
			squarePixels[b] = (obj.avg_square_color[b])+(neighb.avg_square_color[b]);	
			colorSum[b] = obj.sum_color[b] + neighb.sum_color[b];	
			stddevNew[b] = squarePixels[b] - 2*mean[b]*colorSum[b] + areaRes*mean[b]*mean[b];
			
			stddev[b] = Math.sqrt(Math.abs(stddevNew[b])/areaRes);
			wband_norm[b] = _wBand[b]; // TODO: bands must be normalized here or before
			
			color_f[b] = (areaObj* obj.std_color[b]) + (areaNb * neighb.std_color[b]);	
			color_f[b] = wband_norm[b] * ((areaRes*stddev[b])-color_f[b]);
			color_h += color_f[b];
		}
		
		return color_h;
	}
	
	

	private static double calc_spatial_stats(Segment obj, Segment neighb)
	{
		//int [] aux_bbox = new int[4];
		
		double meanX,meanY;
		long squaredPixelsX, squaredPixelsY;
		long pixelSumX, pixelSumY;
		long productPixels;
		double 	polWidth , polLength;
		
		
		double spatial_h, smooth_f, compact_f;
		
		double areaObj, areaNb, areaRes;
		areaObj = obj.getArea();
		areaNb =  neighb.getArea();
		areaRes = areaObj+areaNb;
														
		meanX = ((obj.getMean_x()*areaObj)+(neighb.getMean_x()*areaNb))/areaRes;
		meanY = ((obj.getMean_y()*areaObj)+(neighb.getMean_y()*areaNb))/areaRes;
		squaredPixelsX = obj.getSquare_x() + neighb.getSquare_x();
		squaredPixelsY = obj.getSquare_y() + neighb.getSquare_y();
		pixelSumX = obj.getSum_x() + neighb.getSum_x();
		pixelSumY = obj.getSum_y() + neighb.getSum_y();
		productPixels = obj.getProduct_xy() + neighb.getProduct_xy();
		
		// bounding box length
		//aux_bbox = calc_bounding_box(obj, neighb);
		
		double varX=0, varY=0, covarXY=0;
		varX = (squaredPixelsX - 2*meanX*pixelSumX + areaRes*meanX*meanX)/areaRes;
		varY = (squaredPixelsY - 2*meanY*pixelSumY + areaRes*meanY*meanY)/areaRes;
		covarXY=(productPixels/areaRes)-meanX*meanY;

		polLength = Math.sqrt(8.0* (varX+varY + Math.sqrt(Math.pow(varX-varY,2) +4.0 *Math.pow(covarXY,2))));
		polWidth =  Math.sqrt(8.0* Math.abs(varX+varY - Math.sqrt(Math.pow(varX-varY,2) +4.0 *Math.pow(covarXY,2))));
		
		//corrects errors
		if (polLength == 0)
			polLength = 1.0;

		if (polWidth == 0)
			polWidth = 1.0;
		
		// smoothness factor - alternative SQRT(BB_Area/Area) --> here we multiply Area for pounds pourposes 
		smooth_f = ( Math.sqrt(polWidth*polLength) - 
		  ( Math.sqrt(neighb.getBb_width()*neighb.getBb_length()) + Math.sqrt(obj.getBb_width()*obj.getBb_length())));
		
		// compactness factor - alternative (Fmax/sqrt(Area)) --> here we multiply Area for pounds pourposes 
		compact_f = ( (Math.sqrt(areaRes)*polLength) - 
		  ( (Math.sqrt(areaNb)*neighb.getBb_length()) + (Math.sqrt(areaObj)*obj.getBb_length())));
		// spatial heterogeneity
		spatial_h = (_wCmpt)*compact_f + (1-(_wCmpt))*smooth_f;
	
	  return spatial_h;
	}
	
	/*private static int[] calc_bounding_box(Segment obj, Segment anex){
		int [] aux_bbox = new int [4];
		
		int [] min_row = new int [3];
		int [] max_row = new int [3];
		int [] min_col = new int [3];
		int [] max_col = new int [3];
			
		min_row[0] = obj.getBBoxbyIndex(0);
		max_row[0] = obj.getBBoxbyIndex(0)+obj.getBBoxbyIndex(2);
		min_col[0] = obj.getBBoxbyIndex(1);
		max_col[0] = obj.getBBoxbyIndex(1)+obj.getBBoxbyIndex(3);
		
		min_row[1] = anex.getBBoxbyIndex(0);
		max_row[1] = anex.getBBoxbyIndex(0) + anex.getBBoxbyIndex(2);
		min_col[1] = anex.getBBoxbyIndex(1);
		max_col[1] = anex.getBBoxbyIndex(1)  +anex.getBBoxbyIndex(3);
		
		if (min_row[0]<min_row[1]) min_row[2]=min_row[0]; else min_row[2]=min_row[1];
		if (min_col[0]<min_col[1]) min_col[2]=min_col[0]; else min_col[2]=min_col[1];
		if (max_row[0]>max_row[1]) max_row[2]=max_row[0]; else max_row[2]=max_row[1];
		if (max_col[0]>max_col[1]) max_col[2]=max_col[0]; else max_col[2]=max_col[1];
		
		aux_bbox[0] = min_row[2];
		aux_bbox[1] = min_col[2];
		aux_bbox[2] = max_row[2]-min_row[2];
		aux_bbox[3] = max_col[2]-min_col[2];
		
		return aux_bbox;
	}*/


	private static void mergeBestNeighbors(Segment obj, Segment anex){
		//TODO: Encapsulate operations? 
		//TODO: Some computation is similar to the one existing in calcSpatial and calcSpectral 
		double[] mean = new double[_nbands];
		double[] colorSum = new double[_nbands];
		double[] squarePixels = new double[_nbands];
		double[] stddev = new double[_nbands];
		double[] stddevNew = new double[_nbands];
		
		double areaObj, areaAnex, areaRes;
		//int [] aux_bbox = new int[4];
		
		double meanX,meanY;
		long squaredPixelsX, squaredPixelsY;
		long pixelSumX, pixelSumY;
		long productPixels;
		double 	polWidth , polLength;

		
		areaObj = obj.getArea();
		areaAnex = anex.getArea();
		areaRes = areaObj + areaAnex;
		
		//saves merged area 
		obj.setArea(areaRes);  
		
		//saves spectral values
		for (int b = 0; b <_nbands; b++)
		{
			mean[b] = ((obj.avg_color[b] *areaObj)+(anex.avg_color[b]*areaAnex))/areaRes;
			squarePixels[b] = (obj.avg_square_color[b])+(anex.avg_square_color[b]);	
			colorSum[b] = obj.sum_color[b] + anex.sum_color[b];	
			
			stddevNew[b] = squarePixels[b] - 2*mean[b]*colorSum[b] + areaRes*mean[b]*mean[b];
			stddev[b] = Math.sqrt(Math.abs(stddevNew[b])/areaRes);
			
			//saves merged spectral values
			obj.avg_color[b] = mean[b];
			obj.std_color[b] = stddev[b];
			obj.avg_square_color[b] = squarePixels[b];
			obj.sum_color[b] = colorSum[b];		
		}
		
		//spatial
		if ((1- _wColor) > 0)
		{
	        meanX = ((obj.getMean_x()*areaObj)+(anex.getMean_x()*areaAnex))/areaRes;
	        meanY = ((obj.getMean_y()*areaObj)+(anex.getMean_y()*areaAnex))/areaRes;
	        squaredPixelsX = obj.getSquare_x() + anex.getSquare_x();
	        squaredPixelsY = obj.getSquare_y() + anex.getSquare_y();
	        pixelSumX = obj.getSum_x() + anex.getSum_x();
	        pixelSumY = obj.getSum_y() + anex.getSum_y();
	        productPixels = obj.getProduct_xy() + anex.getProduct_xy();
			
			double varX=0, varY=0, covarXY=0;
			varX = (squaredPixelsX - 2*meanX*pixelSumX + areaRes*meanX*meanX)/areaRes;
			varY = (squaredPixelsY - 2*meanY*pixelSumY + areaRes*meanY*meanY)/areaRes;
			covarXY=(productPixels/areaRes)-meanX*meanY;

			polLength = Math.sqrt(8.0* (varX+varY + Math.sqrt(Math.pow(varX-varY,2) +4.0 *Math.pow(covarXY,2))));
			polWidth =  Math.sqrt(8.0* Math.abs(varX+varY - Math.sqrt(Math.pow(varX-varY,2) +4.0 *Math.pow(covarXY,2))));
			
			//corrects errors
			if (polLength == 0)
				polLength = 1.0;

			if (polWidth == 0)
				polWidth = 1.0;		
			
			//saves merged spatial values
			obj.setMean_x(meanX);
			obj.setMean_y(meanY);
			obj.setSquare_x(squaredPixelsX);
			obj.setSquare_y(squaredPixelsY);
			obj.setSum_x(pixelSumX);
			obj.setSum_y(pixelSumY);
			obj.setProduct_xy(productPixels);
			obj.setBb_length(polLength);
			obj.setBb_width(polWidth);
			
			//aux_bbox = calc_bounding_box(obj, anex );
			//for (int b=0;b<4;b++)
			//{
			//	obj.setBBoxbyIndex(b, aux_bbox[b]);   
			//}
		}
		
		//sets pixels from neighbor as belonging to current segment
		obj.getPixelList().putAll(anex.getPixelList());

		obj.getNeighborIds().remove(anex.getSegment_id());
		anex.getNeighborIds().remove(obj.getSegment_id());		
		
		//update neighborhood
		//TODO: This is not optimized
		for (int id : anex.getNeighborIds()){
			Segment s = _segmentList.get(id);
			
			s.getNeighborIds().remove(anex.getSegment_id());
			//s.neighborIds.remove(s.neighborIds.indexOf(anex.segment_id));
			if (!s.getNeighborIds().contains(obj.getSegment_id())){
				s.getNeighborIds().add(obj.getSegment_id());
			}
			
		}
		obj.getNeighborIds().addAll(anex.getNeighborIds());
		
		//TODO:Do I need to know if it is a border now or just in the end?
		obj.reset_border(_imageW, _imageH);
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
	
	private static double distance(double[] p1, double[] p2)
	{
		double x = p1[0]-p2[0];
		double y = p1[1]-p2[1];
		return (x*x + y*y);
	}
	
	static private void checkEdge(ArrayList< ArrayList<double[]> > rings, int [] outterRing, double CoordX, double CoordY, double CoordX2, double CoordY2){
		double [] point = {CoordX, CoordY};
		double [] point2 = {CoordX2, CoordY2};
		
		if (rings.isEmpty()) {
			ArrayList<double[]> ring = new ArrayList<double[]>();			
			ring.add(point);
			ring.add(point2);			
	        rings.add(ring);
		} 
		else {
	        int foundIndex = -1;
	        int index = 0;
	        for (ArrayList<double[]> ring : rings) {
	        	if (ring.isEmpty()){
	        		index++;
	        		continue;
	        	}
	        	double []  last = ring.get(ring.size()-1);
	        	double []  first = ring.get(0);
	        	
	        	//TODO: Verify this threshold
	            if (distance(last,point)<THRESHOLD) {
	            	//Did not find yet
	                if (foundIndex == -1){
	                    ring.add(point2);
	                	foundIndex=index;
	                	break;
	                }
	            } else if (distance(first,point2)<THRESHOLD) {
	            	//Did not find yet
	            	if (foundIndex == -1){
	            		ring.add(0, point);
		                foundIndex = index;
		                break;
	            	}
	            }
	            index++;
	        }
	        
	        if (foundIndex == -1)
	        {
	        	ArrayList<double[]> ring = new ArrayList<double[]>();	
				ring.add(point);
				ring.add(point2);
		        rings.add(ring);
	        }  else{
	        	index = 0;
		        for (ArrayList<double[]> ring : rings) {
		        	if (ring.isEmpty()){
		        		index++;
		        		continue;
		        	}
		        	
		        	if (index == foundIndex){
		        		index++;
		        		continue;
		        	}
		                
		        	boolean hasOther = false;
		        	
		        	double []  last = ring.get(ring.size()-1);
		        	double []  first = ring.get(0);
		        	
		        	//TODO: Verify this threshold -> THRESHOLD
		            if (distance(last,point)<THRESHOLD) {
		            	hasOther= true;
		            } else if (distance(first,point2)<THRESHOLD) {
		            	hasOther= true;
		            }
		            
		            if (hasOther== true){
	            	    //Has other
	                	joinRings(rings.get(foundIndex),ring);
		            }
		            
		            if (rings.get(foundIndex).isEmpty()){
		            	if (foundIndex == outterRing[0])
		            	{
		            		 outterRing[0] = index;
		            	}
		            	foundIndex = index;
		            }
		            
		            if (ring.isEmpty()){
		            	if (index == outterRing[0])
		            	{
		            		 outterRing[0] = foundIndex;
		            	}
		            }
		            
		            /*if (foundIndex == outterRing[0])
	            	{
	            		if (rings.get(foundIndex).isEmpty())
		                        outterRing[0] = index;
	                } else if (index == outterRing[0]) {
		                    if (ring.isEmpty())
		                        outterRing[0] = foundIndex;
	                } */
		            index++;
		        }
	        }
		}		
	}
	
	private static void joinRings(ArrayList<double[]> r1, ArrayList<double[]> r2)
	{
		double []  lastR1 = r1.get(r1.size()-1);
    	double []  firstR1 = r1.get(0);
    	
    	double []  lastR2 = r2.get(r2.size()-1);
    	double []  firstR2 = r2.get(0);
    	
	    if (distance(lastR1,firstR2)<THRESHOLD) {
	        if (r1.size() >= r2.size()) {
	            r2.remove(0);
	            r1.addAll(r2);
	            r2.clear();
	        } else {
	            r1.remove(r1.size()-1);
	            r2.addAll(0, r1);
	            r1.clear();
	        }
	    } else if (distance(firstR1,lastR2)<THRESHOLD) {
	        if (r2.size() >= r1.size()) {
	        	 r1.remove(0);
		         r2.addAll(r1);
		         r1.clear();
	        } else {
	        	r2.remove(r2.size()-1);
	            r1.addAll(0, r2);
	            r2.clear();
	        }
	    } else if (distance(lastR1,lastR2)<THRESHOLD) {

	        if (r1.size() > r2.size()) {
	        	r2.remove(r2.size()-1);
	            r1.addAll(0, r2);
	            r2.clear();
	        } else {
	        	r1.remove(r1.size()-1);
	            r2.addAll(0, r1);
	            r1.clear();
	        }

	    } else if (distance(firstR1,firstR2)<THRESHOLD) {
	        if (r1.size() > r2.size()) {
	        	r2.remove(0);
	            r1.addAll(0,r2);
	            r2.clear();
	        } else {
	        	r1.remove(0);
	            r2.addAll(0,r1);
	            r1.clear();
	        }
	    }

	}
	
}
