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

package br.puc_rio.ele.lvc.interimage.core.datamanager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import br.puc_rio.ele.lvc.interimage.common.Tile;
import br.puc_rio.ele.lvc.interimage.common.TileManager;
import br.puc_rio.ele.lvc.interimage.common.URL;
import br.puc_rio.ele.lvc.interimage.data.Image;
import br.puc_rio.ele.lvc.interimage.data.ImageConverter;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySet;
import br.puc_rio.ele.lvc.interimage.geometry.Shape;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;

/**
 * A class that holds the information about the data used in an interpretation project. 
 * @author Rodrigo Ferreira
 * 
 * TODO: Maybe using a list of geobboxes is better. It could avoid 'idle' tiles for sparse data.
 * TODO: Treat remote locations in project URLs
 */
public class DataManager {

	private Source _source;
	private double[] _geoBBox; //west, south, east, north
	
	public DataManager() {		
		_geoBBox = new double[4];
		_geoBBox[0] = Double.MAX_VALUE;
		_geoBBox[1] = Double.MAX_VALUE;
		_geoBBox[2] = -Double.MAX_VALUE;
		_geoBBox[3] = -Double.MAX_VALUE;
	}
	
	public void setup(Properties props) {
		String service = props.getProperty("interimage.storageService");
		
		if (service.equals("AWS"))
			_source = new AWSSource(props.getProperty("interimage.aws.accessKey"),props.getProperty("interimage.aws.secretKey"),props.getProperty("interimage.aws.S3Bucket"));
	}
	
	public void updateGeoBBox(double[] gbox) {
		if (gbox[0] < _geoBBox[0]) {	//west
			_geoBBox[0] = gbox[0];
		} 
		
		if (gbox[1] < _geoBBox[1]) {	//south
			_geoBBox[1] = gbox[1];
		}
		
		if (gbox[2] > _geoBBox[2]) {	//east
			_geoBBox[2] = gbox[2];
		}
		
		if (gbox[3] > _geoBBox[3]) {	//north
			_geoBBox[3] = gbox[3];
		}
				
	}
		
	public String setupResource(Resource resource, TileManager tileManager, String projectPath) {
		
		String returnUrl = null;
		
		if (resource instanceof DefaultResource) {
			
			DefaultResource rsrc = (DefaultResource)resource;
			
			if (rsrc.getType() == DefaultResource.TILE) {
				
				try {
									
					@SuppressWarnings("unchecked")
					List<Tile> tiles = (List<Tile>)rsrc.getObject();
					
					OutputStream stream = new FileOutputStream(projectPath + "tiles.ser");
				    ObjectOutputStream out = new ObjectOutputStream(stream);
					
				    out.writeObject(tiles);
				    
				    out.close();
				    
				    //TODO: Just for test purposes
				    FileWriter fw = new FileWriter(projectPath + "tiles.wkt");
					BufferedWriter bw = new BufferedWriter(fw);
				    				    
				    for (Tile tile : tiles) {
				    	bw.write(tile.getGeometry() + "\n");
				    }
				    
				    bw.close();
				    
				    String to = "resources/tiles.ser";
				    
				    _source.put(projectPath + "tiles.ser", to, rsrc);
				    
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type TILE; error - " + e.getMessage());
				}
				
			} else if (rsrc.getType() == DefaultResource.FUZZY_SET) {
				
				try {
					
					@SuppressWarnings("unchecked")
					List<FuzzySet> fuzzySets = (List<FuzzySet>)rsrc.getObject();
					
					OutputStream stream = new FileOutputStream(projectPath + "fuzzysets.ser");
				    ObjectOutputStream out = new ObjectOutputStream(stream);
					
				    out.writeObject(fuzzySets);
				    
				    out.close();
				    
				    String to = "resources/fuzzysets.ser";
				    
				    _source.put(projectPath + "fuzzysets.ser", to, rsrc);
				    
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type FUZZY_SET; error - " + e.getMessage());
				}
				
			} else if (rsrc.getType() == DefaultResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
								
				if (url.contains(".csv")) {
										
					String to = "resources/" + shp.getKey() + ".csv";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".wkt")) {
				
					//TODO: Convert to internal CRS					
					
					String to = "resources/shapes/" + shp.getKey() + ".wkt";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".shp")) {
										
					String wkt = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".wkt";
					ShapefileConverter.shapefileToWKT(url, wkt, shp.getCRS(), shp.getCRS());
					
					String to = "resources/shapes/" + shp.getKey() + ".wkt";
					
					_source.put(wkt, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				}
				
			} else if (rsrc.getType() == DefaultResource.SEMANTIC_NETWORK) {
				
			} else if (rsrc.getType() == DefaultResource.PROPERTY) {
			
				/*try {
				
					Properties props = (Properties)rsrc.getObject();
					
					FileWriter fw = new FileWriter(projectPath + "interimage-public.properties");
					BufferedWriter bw = new BufferedWriter(fw);
					
					for (Map.Entry<Object, Object> entry : props.entrySet()) {
						String key = (String)entry.getKey();
						String value = (String)entry.getValue();
			    		if (!key.startsWith("interimage.local")) {
			    			bw.write(key + "=" + value + "\n");
			    		}
			    	}
					
					bw.close();
					
					_source.put(projectPath + "interimage-public.properties", "resources/" + "interimage-public.properties");
					
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type PROPERTY; error - " + e.getMessage());
				}*/
				
			} else if (rsrc.getType() == DefaultResource.FILE) {
				
				String url = (String)rsrc.getObject();
				
				String path = URL.getPath(url);
				
				String name = URL.getFileName(url);
				
				if (url.contains(".pig")) {
				
					_source.put(path + name, "resources/scripts/" + name, rsrc);
					
				} else if (url.contains(".jar")) {
					
					_source.put(path + name, "resources/libs/" + name, rsrc);
					
				}
				
			}
			
		} else if (resource instanceof SplittableResource) {
			
			SplittableResource rsrc = (SplittableResource)resource;
			
			if (rsrc.getType() == SplittableResource.IMAGE) {
				
				Image img = (Image)rsrc.getObject();
				
				String key = img.getKey();
				
				String url = img.getURL();
				
				//TODO: treat other formats
				if ((url.endsWith(".tif")) || (url.endsWith(".tiff"))) {
										
					ImageConverter.ImageToJSON(img, projectPath + "images/" + key + "/", null, true, tileManager);
					
					File folder = new File(projectPath + "images/" + key + "/");
					
					for (final File fileEntry : folder.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	_source.put(projectPath + "images/" + key + "/" + fileEntry.getName(), "resources/images/" + fileEntry.getName(), rsrc);
				        }
				    }
					
				}
								
			} else if (rsrc.getType() == SplittableResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
								
				if (url.contains(".shp")) {
					
					double[] gbox = new double[4];

					//TODO: Should become a parameter
					List<String> list = new ArrayList<String>();
					list.add("object_id_");
					list.add("class");
					list.add("file");
					list.add("fileoWest");
					list.add("fileoNorth");
					list.add("fileoEast");
					list.add("fileoSouth");
					list.add("id");
					list.add("llx");
					list.add("lly");
					list.add("urx");
					list.add("ury");
					
					String json = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".json";
					ShapefileConverter.shapefileToJSON(url, json, list, false, shp.getCRS(), shp.getCRS(), gbox, tileManager);
					//ShapefileConverter.JSONToShapefile(json, "C:\\Users\\Rodrigo\\Desktop\\test.shp", list, false, shp.getCRS(), shp.getCRS());
					String to = "resources/shapes/" + shp.getKey() + ".json";
					
					_source.put(json, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
					/*System.out.println(gbox[0]);
					System.out.println(gbox[1]);
					System.out.println(gbox[2]);
					System.out.println(gbox[3]);*/
					
					/*Updating the global bbox*/
					updateGeoBBox(gbox);
					
				} else if (url.contains(".json")) {
					
				} else if (url.contains(".wkt")) {
					
				} else if (url.contains(".kml")) {
					
				} else if (url.contains(".osm")) {
					
				}
				
			}
			
		}
		
		return returnUrl;
		
	}
	
	public double[] getGeoBBox() {
		return _geoBBox;
	}
	
	public String getSourceURL() {
		return _source.getURL();
	}
	
}
