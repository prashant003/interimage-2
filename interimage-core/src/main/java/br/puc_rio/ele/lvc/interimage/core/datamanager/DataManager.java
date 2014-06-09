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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import br.puc_rio.ele.lvc.interimage.common.URL;
import br.puc_rio.ele.lvc.interimage.data.Image;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySet;
import br.puc_rio.ele.lvc.interimage.geometry.Shape;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;
import br.puc_rio.ele.lvc.interimage.geometry.Tile;
import br.puc_rio.ele.lvc.interimage.geometry.TileManager;

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
		_source = new AWSSource("access-key","secret-key","interimage2");
		_geoBBox = new double[4];
		_geoBBox[0] = Double.MAX_VALUE;
		_geoBBox[1] = Double.MAX_VALUE;
		_geoBBox[2] = -Double.MAX_VALUE;
		_geoBBox[3] = -Double.MAX_VALUE;
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
		
	public void setupResource(Resource resource, TileManager tileManager, String projectPath) {
		
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
				    
				    _source.put(projectPath + "tiles.ser", "resources/tiles.ser");
				    
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
				    
				    _source.put(projectPath + "fuzzysets.ser", "resources/fuzzysets.ser");
				    
				} catch (Exception e) {
					System.out.println("Failed to setup DefaultResource of type FUZZY_SET; error - " + e.getMessage());
				}
				
			} else if (rsrc.getType() == DefaultResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
								
				if (url.contains(".csv")) {
										
					_source.put(url, "resources/" + shp.getKey() + ".csv");
					
				} else if (url.contains(".wkt")) {
				
					//TODO: Convert to internal CRS					
					_source.put(url, "resources/shapes/" + shp.getKey() + ".wkt");
					
				} else if (url.contains(".shp")) {
										
					String wkt = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".wkt";
					ShapefileConverter.shapefileToWKT(url, wkt, shp.getEPSG());
					_source.put(wkt, "resources/shapes/" + shp.getKey() + ".wkt");
					
				}
				
			} else if (rsrc.getType() == DefaultResource.SEMANTIC_NETWORK) {
				
			}
			
		} else if (resource instanceof SplittableResource) {
			
			SplittableResource rsrc = (SplittableResource)resource;
			
			if (rsrc.getType() == SplittableResource.IMAGE) {
				
				Image img = (Image)rsrc.getObject();
				
				String url = img.getURL();
												
				if (url.contains(".tif")) {
				
					//TODO: convert and upload
					
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
					
					String json = URL.getPath(url) + URL.getFileNameWithoutExtension(url) + ".json";
					ShapefileConverter.shapefileToJSON(url, json, list, false, shp.getEPSG(), gbox, tileManager);					
					_source.put(json, "resources/shapes/" + shp.getKey() + ".json");
					
					/*Updating the global bbox*/
					updateGeoBBox(gbox);
										
				} else if (url.contains(".json")) {
					
				} else if (url.contains(".wkt")) {
					
				} else if (url.contains(".kml")) {
					
				} else if (url.contains(".osm")) {
					
				}
				
			}
			
		}
		
	}
	
	public double[] getGeoBBox() {
		return _geoBBox;
	}
	
}
