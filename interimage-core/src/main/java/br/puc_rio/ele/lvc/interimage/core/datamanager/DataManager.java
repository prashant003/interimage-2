<<<<<<< HEAD
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
import br.puc_rio.ele.lvc.interimage.common.UUID;
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
		
	public String setupResource(Resource resource, TileManager tileManager, String projectName, String projectPath) {
		
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
				    	
					/*Cleaning folder*/
					File path = new File(projectPath + "tiles/");
					
					path.mkdirs();
					
					for (final File fileEntry : path.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	fileEntry.delete();
				        }
				    }
					
				    for (Tile tile : tiles) {
				    	bw.write(tile.getGeometry() + "\n");
				    	
				    	/*for segmentation purposes*/
					    OutputStream out2 = new FileOutputStream(projectPath + "tiles/" + tile.getCode() + ".json");
				    	
				    	String id = new UUID(null).random();
				    	
				    	String str = "{\"geometry\":";	                
		                str += "\"" + tile.getGeometry() + "\"";
		                //str += "\"" + WKBWriter.toHex(new WKBWriter().write(geom)) + "\"";	                	                
		                str += ",\"data\":{\"0\":\"\"}";
		                str += ",\"properties\":{\"tile\":\"" + tile.getCode() + "\",\"crs\":\"" + tileManager.getCRS() + "\",\"class\":\"None\",\"iiuuid\":\"" + id + "\"}}\n";
				    	out2.write(str.getBytes());
				    	
				    	out2.close();
				    }
				    
				    bw.close();
				    
				    ShapefileConverter.WKTToShapefile(projectPath + "tiles.wkt", projectPath + "tiles.shp", null, null);
				    
				    File folder = new File(projectPath + "tiles/");
					
					for (final File fileEntry : folder.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	_source.put(projectPath + "tiles/" + fileEntry.getName(), "interimage/" + projectName + "/tiles/" + fileEntry.getName(), rsrc);
				        }
				    }
				    
				    String to = "interimage/" + projectName + "/resources/tiles.ser";
				    
				    _source.put(projectPath + "tiles.ser", to, rsrc);
				    			
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type TILE; error - " + e.getMessage());
					e.printStackTrace();
				}
				
			} else if (rsrc.getType() == DefaultResource.FUZZY_SET) {
				
				try {
					
					@SuppressWarnings("unchecked")
					List<FuzzySet> fuzzySets = (List<FuzzySet>)rsrc.getObject();
					
					OutputStream stream = new FileOutputStream(projectPath + "fuzzysets.ser");
				    ObjectOutputStream out = new ObjectOutputStream(stream);
					
				    out.writeObject(fuzzySets);
				    
				    out.close();
				    
				    String to = "interimage/" + projectName + "/resources/fuzzysets.ser";
				    
				    _source.put(projectPath + "fuzzysets.ser", to, rsrc);
				    
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type FUZZY_SET; error - " + e.getMessage());
				}
				
			} else if (rsrc.getType() == DefaultResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
				
				if (url.contains(".csv")) {
				
					//TODO: This shouldn't be here
					
					String to = "interimage/" + projectName + "/resources/" + shp.getKey() + ".csv";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".wkt")) {
					
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".wkt";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".shp")) {
										
					String wkt = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".wkt";
					ShapefileConverter.shapefileToWKT(url, wkt, shp.getCRS(), shp.getCRS());
					
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".wkt";
					
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
				
					_source.put(path + name, "interimage/scripts/" + name, rsrc);
					
				} else if (url.contains(".jar")) {
					
					_source.put(path + name, "interimage/lib/" + name, rsrc);
					
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
										
					File folder = new File(projectPath + "images/" + key);
					
					if (!folder.exists()) {
					
						ImageConverter.ImageToJSON(img, projectPath + "images/" + key + "/", null, true, tileManager);
					
					}
					
					for (final File fileEntry : folder.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	if (!fileEntry.getName().endsWith("w"))
				        		_source.put(projectPath + "images/" + key + "/" + fileEntry.getName(), "interimage/" + projectName + "/resources/images/" + key + "/" + fileEntry.getName(), rsrc);
				        		//_source.makePublic("interimage/" + projectName + "/resources/images/" + key + "/" + fileEntry.getName());
				        }
				    }
					
					//_source.multiplePut(folder, "interimage/" + projectName + "/resources/images/" + key + "/");
					
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
					ShapefileConverter.shapefileToJSON(url, json, list, false, shp.getCRS(), shp.getCRS(), gbox, tileManager,true);
					//ShapefileConverter.JSONToShapefile(json, "C:\\Users\\Rodrigo\\Desktop\\test.shp", list, false, shp.getCRS(), shp.getCRS());
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".json";
					
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
	
	public String getSourceSpecificURL() {
		return _source.getSpecificURL();
	}
	
	public String getSourceURL() {
		return _source.getURL();
	}
	
	public void close() {
		_source.close();
	}
	
}
=======
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
import br.puc_rio.ele.lvc.interimage.common.UUID;
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
		
	public String setupResource(Resource resource, TileManager tileManager, String projectName, String projectPath) {
		
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
				    	
					/*Cleaning folder*/
					File path = new File(projectPath + "tiles/");
					
					path.mkdirs();
					
					for (final File fileEntry : path.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	fileEntry.delete();
				        }
				    }
					
				    for (Tile tile : tiles) {
				    	bw.write(tile.getGeometry() + "\n");
				    	
				    	/*for segmentation purposes*/
					    OutputStream out2 = new FileOutputStream(projectPath + "tiles/" + tile.getCode() + ".json");
				    	
				    	String id = new UUID(null).random();
				    	
				    	String str = "{\"geometry\":";	                
		                str += "\"" + tile.getGeometry() + "\"";
		                //str += "\"" + WKBWriter.toHex(new WKBWriter().write(geom)) + "\"";	                	                
		                str += ",\"data\":{\"0\":\"\"}";
		                str += ",\"properties\":{\"tile\":\"" + tile.getCode() + "\",\"crs\":\"" + tileManager.getCRS() + "\",\"class\":\"None\",\"iiuuid\":\"" + id + "\"}}\n";
				    	out2.write(str.getBytes());
				    	
				    	out2.close();
				    }
				    
				    bw.close();
				    
				    ShapefileConverter.WKTToShapefile(projectPath + "tiles.wkt", projectPath + "tiles.shp", null, null);
				    
				    File folder = new File(projectPath + "tiles/");
					
					for (final File fileEntry : folder.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	_source.put(projectPath + "tiles/" + fileEntry.getName(), "interimage/" + projectName + "/tiles/" + fileEntry.getName(), rsrc);
				        }
				    }
				    
				    String to = "interimage/" + projectName + "/resources/tiles.ser";
				    
				    _source.put(projectPath + "tiles.ser", to, rsrc);
				    			
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type TILE; error - " + e.getMessage());
					e.printStackTrace();
				}
				
			} else if (rsrc.getType() == DefaultResource.FUZZY_SET) {
				
				try {
					
					@SuppressWarnings("unchecked")
					List<FuzzySet> fuzzySets = (List<FuzzySet>)rsrc.getObject();
					
					OutputStream stream = new FileOutputStream(projectPath + "fuzzysets.ser");
				    ObjectOutputStream out = new ObjectOutputStream(stream);
					
				    out.writeObject(fuzzySets);
				    
				    out.close();
				    
				    String to = "interimage/" + projectName + "/resources/fuzzysets.ser";
				    
				    _source.put(projectPath + "fuzzysets.ser", to, rsrc);
				    
				    returnUrl = _source.getURL() + to;
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type FUZZY_SET; error - " + e.getMessage());
				}
				
			} else if (rsrc.getType() == DefaultResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
				
				if (url.contains(".csv")) {
				
					//TODO: This shouldn't be here
					
					String to = "interimage/" + projectName + "/resources/" + shp.getKey() + ".csv";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".wkt")) {
					
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".wkt";
					
					_source.put(url, to, rsrc);
					
					returnUrl = _source.getURL() + to;
					
				} else if (url.contains(".shp")) {
										
					String wkt = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".wkt";
					ShapefileConverter.shapefileToWKT(url, wkt, shp.getCRS(), shp.getCRS());
					
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".wkt";
					
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
				
					_source.put(path + name, "interimage/scripts/" + name, rsrc);
					
				} else if (url.contains(".jar")) {
					
					_source.put(path + name, "interimage/lib/" + name, rsrc);
					
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
										
					File folder = new File(projectPath + "images/" + key);
					
					if (!folder.exists()) {
					
						ImageConverter.ImageToJSON(img, projectPath + "images/" + key + "/", null, true, tileManager);
					
					}
					
					for (final File fileEntry : folder.listFiles()) {
				        if (fileEntry.isDirectory()) {
				        	//ignore
				        } else {
				        	if (!fileEntry.getName().endsWith("w"))
				        		_source.put(projectPath + "images/" + key + "/" + fileEntry.getName(), "interimage/" + projectName + "/resources/images/" + key + "/" + fileEntry.getName(), rsrc);
				        		//_source.makePublic("interimage/" + projectName + "/resources/images/" + key + "/" + fileEntry.getName());
				        }
				    }
					
					//_source.multiplePut(folder, "interimage/" + projectName + "/resources/images/" + key + "/");
					
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
					ShapefileConverter.shapefileToJSON(url, json, list, false, shp.getCRS(), shp.getCRS(), gbox, tileManager,true);
					//ShapefileConverter.JSONToShapefile(json, "C:\\Users\\Rodrigo\\Desktop\\test.shp", list, false, shp.getCRS(), shp.getCRS());
					String to = "interimage/" + projectName + "/resources/shapes/" + shp.getKey() + ".json";
					
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
	
	public String getSourceSpecificURL() {
		return _source.getSpecificURL();
	}
	
	public String getSourceURL() {
		return _source.getURL();
	}
	
	public void close() {
		_source.close();
	}
	
}
>>>>>>> branch 'master' of https://github.com/darioaugusto/interimage-3
