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

import br.puc_rio.ele.lvc.interimage.common.URL;
import br.puc_rio.ele.lvc.interimage.geometry.Shape;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;
import br.puc_rio.ele.lvc.interimage.geometry.WebMercatorLatLongConverter;

public class DataManager {

	private double _minResolution;
	private double _geoTileSize;
	private int _numTilesX;
	private int _numTilesY;
	private Source _source;
	
	public DataManager() {
		_source = new AWSSource("AKIAI4FY552KCQ5LC7EA","eErjwkB1ps6ILm2QzuDqUIzLrwI04EmibcC1E9wg","interimage2");
	}
	
	public void setMinResolution(double minRes) {
		_minResolution = minRes;
		
		WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
		webMercator.setDatum("WGS84");
    			
    	//TODO: Make it a parameter
    	_geoTileSize = 256 * _minResolution;
    	
    	_numTilesX = (int)Math.ceil((webMercator.getGeoEast()-webMercator.getGeoWest()) / _geoTileSize);
    	
    	_numTilesY = (int)Math.ceil((webMercator.getGeoNorth()-webMercator.getGeoSouth()) / _geoTileSize);
		
	}
	
	public double getMinResolution() {
		return _minResolution;
	}
	
	public double getGeoTileSize() {
		return _geoTileSize;
	}
	
	public int getNumTilesX() {
		return _numTilesX;
	}
	
	public int getNumTilesY() {
		return _numTilesY;
	}
	
	public void setupResource(Resource resource) {
		
		if (resource instanceof DefaultResource) {
			
		} else if (resource instanceof SplittableResource) {
			
			SplittableResource rsrc = (SplittableResource)resource;
			
			if (rsrc.getType() == SplittableResource.IMAGE) {
				
				//Image img = (Image)rsrc.getObject();
								
			} else if (rsrc.getType() == SplittableResource.SHAPE) {
				
				Shape shp = (Shape)rsrc.getObject();
				
				String url = shp.getURL();
								
				if (url.contains(".shp")) {
								
					//TODO: download if it's a remote location 
					
					String json = new String();
					ShapefileConverter.shapefileToJSON(url, json, null, true);
					String fileName = URL.getFileName(json);
					_source.put(json, "/resources/shapes/" + fileName);
					
				} else if (url.contains(".json")) {
					
				} else if (url.contains(".wkt")) {
					
				} else if (url.contains(".kml")) {
					
				} else if (url.contains(".osm")) {
					
				}
				
			}
			
		}
		
	}
	
}
