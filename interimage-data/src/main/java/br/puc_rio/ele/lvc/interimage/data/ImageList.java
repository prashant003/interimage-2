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

package br.puc_rio.ele.lvc.interimage.data;

import java.util.HashMap;
import java.util.Map;

/**
 * A class that holds the information about a list of image resources.
 * @author Rodrigo Ferreira
 */
public class ImageList {

	private double _geoWest, _geoNorth, _geoEast, _geoSouth;
	private Map<String,Image> _images;
	
	public ImageList() {
		_geoWest = Double.MAX_VALUE;
		_geoNorth = -Double.MAX_VALUE;
		_geoEast = -Double.MAX_VALUE;
		_geoSouth = Double.MAX_VALUE;
		_images = new HashMap<String,Image>(); 
	}

	public void add(String key, Image image) {
		_images.put(key, image);
		
		if (image.getGeoWest() < _geoWest)
			_geoWest = image.getGeoWest();
		
		if (image.getGeoNorth() > _geoNorth)
			_geoNorth = image.getGeoNorth();
		
		if (image.getGeoEast() > _geoEast)
			_geoEast = image.getGeoEast();
		
		if (image.getGeoSouth() < _geoSouth)
			_geoSouth = image.getGeoSouth();
		
	}
	
	public double getGeoWest() {
		return _geoWest;
	}
	
	public double getGeoNorth() {
		return _geoNorth;
	}
	
	public double getGeoEast() {
		return _geoEast;
	}
	
	public double getGeoSouth() {
		return _geoSouth;
	}
	
	public int size() {
		return _images.size();
	}
	
	public Map<String,Image> getImages() {
		return _images;
	}
	
}
