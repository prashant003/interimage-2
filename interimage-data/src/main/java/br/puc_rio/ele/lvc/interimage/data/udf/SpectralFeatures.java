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

package br.puc_rio.ele.lvc.interimage.data.udf;

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
import java.util.Iterator;
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
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;

import br.puc_rio.ele.lvc.interimage.common.Common;
import br.puc_rio.ele.lvc.interimage.common.TileManager;
import br.puc_rio.ele.lvc.interimage.data.imageioimpl.plugins.tiff.TIFFImageReader;
import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.data.FeatureCalculator;
import br.puc_rio.ele.lvc.interimage.data.Image;

/**
 * A class that computes spectral features for all the input polygons. 
 * @author Rodrigo Ferreira
 */
public class SpectralFeatures extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	
	private String _imageUrl;
	private String _features;
	private Map<String, Map<String, Map<String, Object>>> _imageMap;	//image -> (tile, obj)
	private Map<String, Map<String, Object>> _featureMap;	//attribute, operation, params
	private List<String> _images;
	private TileManager _tileManager;
	private double _tileSize;
	private Long _currentTileId;
	
	/**Constructor that takes image URL, feature list and tile size.*/
	public SpectralFeatures(String imageUrl, String features, String tileSize) {
		_imageUrl = imageUrl;
		_features = features;
		_tileSize = Double.parseDouble(tileSize);
	}
	
	private void parseFeatures() {
		
		_images = new ArrayList<String>();		
		_featureMap = new HashMap<String, Map<String, Object>>();
		
		String[] expressions = _features.split(";");
		
		for (int i=0; i<expressions.length; i++) {
			
			int idx = expressions[i].indexOf("=");
			
			String term1 = expressions[i].substring(0,idx).trim();
			
			String term2 = expressions[i].substring(idx+1).trim();
			
			int idx1 = term2.indexOf("(");
			
			String name = term2.substring(0,idx1).trim();
			
			String list = term2.substring(idx1+1,term2.length()-1).trim();
			
			String[] params = list.split(",");
						
			List<String> paramList = new ArrayList<String>();
			
			for (int j=0; j<params.length; j++) {
				
				String[] tokens = params[j].split("_");
				
				String imageKey = "";
				
				if (tokens.length>0) {
					imageKey = tokens[0];
				} else if (!Common.isNumeric(tokens[0])) {
					imageKey = tokens[0];
				}
				
				if (!_images.contains(imageKey)) {
					_images.add(imageKey);
				}
				
				/*if (params[j].indexOf("_") != -1) {
					String[] tokens = params[j].split("_");
					
					if (!_images.contains(tokens[0])) {
						_images.add(tokens[0]);
						//_imageMap.put(tokens[0], new HashMap<String, BufferedImage>());
					}
					
				}*/
				
				paramList.add(params[j]);
							
			}
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put("operation", name);
			map.put("params", paramList);
			
			_featureMap.put(term1, map);
						
		}
		
	}
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a bag
     * @exception java.io.IOException
     * @return a bag with the computed features in the properties
     */
	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() == 0)
            return null;
			
		try {
		
			DataBag bag = DataType.toBag(input.get(0));
			
			DataBag output = BagFactory.getInstance().newDefaultBag();
			
			Iterator it = bag.iterator();
	        while (it.hasNext()) {
	            Tuple t = (Tuple)it.next();
	            
	            Object objGeometry = t.get(0);
				Map<String,String> data = (Map<String,String>)t.get(1);
				Map<String,Object> properties = DataType.toMap(t.get(2));
							
				String tileStr = DataType.toString(properties.get("tile"));
				
				String crs = DataType.toString(properties.get("crs"));
				
				long tileId = Long.parseLong(tileStr.substring(1));
				
				Geometry geometry = _geometryParser.parseGeometry(objGeometry);
				
				//TODO: maybe it's not necessary to compute the tiles
				//List<String> tiles = _tileManager.getTiles(new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()});
				
				if (_featureMap == null) {
					parseFeatures();
					_tileManager = new TileManager(_tileSize, crs);
				}
				
				List<String> tiles = new ArrayList<String>();
				//TODO: Some intelligence here can help to define the tiles to be considered
				tiles.add("T" + tileId);
				tiles.add("T" + (tileId+1));
				tiles.add("T" + (tileId+_tileManager.getNumTilesX()));
				tiles.add("T" + (tileId+_tileManager.getNumTilesX()+1));
				
				//TODO: Think about multiple assignment, works for single 
								
				//Map<String,Image> imageObjects = new HashMap<String,Image>();
				
				if (_currentTileId == null)
					_currentTileId = (long)-1;
				
				if (_currentTileId != tileId) {
									
					_imageMap = new HashMap<String, Map<String, Map<String, Object>>>();
					
					for (String img : _images) {
						
						Image imageObj = new Image();

						if (!_imageMap.containsKey(img)) {
							_imageMap.put(img, new HashMap<String, Map<String, Object>>());
						}
							
						//System.out.println(img);
						
						//int tileIndex = 0;
						
						for (String tile : tiles) {
						
							if (!_imageMap.get(img).containsKey(tile)) {	//if tile is not yet in the map
								
								//System.out.println(tile + " is not in the map");
								
								//download the tile
								BufferedImage buff = null;
								//Img< DoubleType > buff = null;
								
								if (br.puc_rio.ele.lvc.interimage.common.URL.exists(_imageUrl + img + "_" + tile + ".tif")) {	//if tile doesn't exist
								
									//System.out.println("tile exists");

									//ImgOpener imgOpener = new ImgOpener();

									//buff = (Img< DoubleType >) imgOpener.openImg( _imageUrl + img + "_" + tile + ".tif");
									
									//TODO: treat other formats
									URL imageFile  = new URL(_imageUrl + img + "_" + tile + ".tif");	        		
									
									URLConnection urlConn2 = imageFile.openConnection();
																		
					                urlConn2.connect();
					                
					                InputStream stream = new BufferedInputStream(urlConn2.getInputStream());
														                
									//Preparing to read the image
							        ImageInputStream in = ImageIO.createImageInputStream(stream);
							        
							        if (in == null)
							        	throw new Exception("Could not create input stream: " + imageFile.toString());
							        
							        // try to decode the image with all registered imagereaders 
							        //Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
							        //Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("tiff");
							        
							        //if (!readers.hasNext())
							        //	throw new Exception("Could not find a reader: " + imageFile.toString());
							        
							        //ImageReader reader = null;
							        
							        TIFFImageReader reader = new TIFFImageReader(null);
							        reader.setInput(in);
							        
							        //if (readers.hasNext()) {
							        //    reader = readers.next();
							        //  reader.setInput(in);           
							        //} else {
							        //	// TODO: Test if it is supported format
							        //	//out.close();
								    //    throw new Exception("Unsuported image type");
							        //}
									
							        ImageReadParam param = reader.getDefaultReadParam();
							        
									buff = reader.read(0,param);
					                
									if (buff == null)
										throw new Exception("Could not instantiate tile image: " + imageFile.toString());
									
									URL worldFile = new URL(_imageUrl + img + "_" + tile + ".tifw");
									URLConnection urlConn = worldFile.openConnection();
					                urlConn.connect();
									InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
							        BufferedReader reader2 = new BufferedReader(inStream);
							        
							        double[] worldArr = new double[6];
							        
							        String line;
							        int index = 0;
							        while ((line = reader2.readLine()) != null) {
							        	if (!line.trim().isEmpty()) {
								        	worldArr[index] = Double.parseDouble(line);
								        	index++;
							        	}
							        }
									
					                //after downloading, update the map
									Map<String, Object> aux2 = new HashMap<String, Object>();
									
									double[] tileGeoBox = new double[4];
									
									double [] origin = new double[] {worldArr[4] - (worldArr[0]/2), worldArr[5] - (worldArr[3]/2)};
																		
									tileGeoBox[0] = origin[0];
									tileGeoBox[1] = origin[1] + (worldArr[3]*buff.getHeight());
									tileGeoBox[2] = origin[0] + (worldArr[0]*buff.getWidth());
									tileGeoBox[3] = origin[1];
									
									aux2.put("geoBox",tileGeoBox);
									aux2.put("image",buff);
									
									/*
									 * Tile index
									 * 
									 * 2 | 3
									 * - - -
									 * 0 | 1
									 * 
									 */
									
									//System.out.println("adds " + tileIndex);
									
									Map<String, Map<String, Object>> aux = _imageMap.get(img);
									aux.put(tile, aux2);
					                //aux.put(String.valueOf(tileIndex), aux2);
					                //tileIndex++;
					                
								}
				                
							}
							
						}
				        
						/*URL worldFile = new URL(_imageUrl + img + ".tifw");
						URLConnection urlConn = worldFile.openConnection();
		                urlConn.connect();
						InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
				        BufferedReader reader = new BufferedReader(inStream);
				        				 
				        imageObj.setKey(img);
				        
				        String line;
				        int index = 0;
				        while ((line = reader.readLine()) != null) {
				        	if (!line.trim().isEmpty()) {
				        		if (index==0)
				        			imageObj.setBands(Integer.parseInt(line));
				        		else if (index==1)
				        			imageObj.setRows(Integer.parseInt(line));
				        		else if (index==2)
				        			imageObj.setCols(Integer.parseInt(line));
				        		else if (index==3)
				        			imageObj.setGeoWest(Double.parseDouble(line));
				        		else if (index==4)
				        			imageObj.setGeoSouth(Double.parseDouble(line));
				        		else if (index==5)
				        			imageObj.setGeoEast(Double.parseDouble(line));
				        		else if (index==6)
				        			imageObj.setGeoNorth(Double.parseDouble(line));
					        	index++;
				        	}
				        }
				        
				        imageObjects.put(img, imageObj);*/
				        
					}
				
					_currentTileId = tileId;
					
				}
								
				Map<String, Double> features = new HashMap<String, Double>();
				
				//String iiuuid = DataType.toString(properties.get("iiuuid"));
				
				//if (iiuuid.equals("84c425af-3957-424b-9883-92c174ccad8d")) {
					features = new FeatureCalculator().computeFeatures(_imageMap, _featureMap, geometry);					
				//}
								
				for (Map.Entry<String, Double> entry : features.entrySet()) {
					properties.put(entry.getKey(), entry.getValue());
				}
				
				output.add(t);
				
	        }
			
			return output;
			
		} catch (Exception e) {
			e.printStackTrace();
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
