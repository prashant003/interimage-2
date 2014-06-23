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

/**
 * A class that holds the information about an image resource.
 * @author Rodrigo Ferreira
 */
public class Image {

	private String _key;
	private boolean _default;
	private String _url;
	private double _geoWest, _geoNorth, _geoEast, _geoSouth;
	private int _cols, _rows;
	private int _bands;
	private String _crs;
	
	public void setKey(String key) {
		_key = key;
	}
	
	public String getKey() {
		return _key;
	}
	
	public void setDefault(boolean def) {
		_default = def;
	}
	
	public boolean getDefault() {
		return _default;
	}
	
	public void setURL(String url) {
		_url = url;
	}
	
	public String getURL() {
		return _url;
	}
	
	public void setGeoWest(double geoWest) {
		_geoWest = geoWest;
	}
	
	public double getGeoWest() {
		return _geoWest;
	}
	
	public void setGeoNorth(double geoNorth) {
		_geoNorth = geoNorth;
	}
	
	public double getGeoNorth() {
		return _geoNorth;
	}
	
	public void setGeoEast(double geoEast) {
		_geoEast = geoEast;
	}
	
	public double getGeoEast() {
		return _geoEast;
	}
	
	public void setGeoSouth(double geoSouth) {
		_geoSouth = geoSouth;
	}
	
	public double getGeoSouth() {
		return _geoSouth;
	}
	
	public void setRows(int rows) {
		_rows = rows;
	}
	
	public int getRows() {
		return _rows;
	}
	
	public void setCols(int cols) {
		_cols = cols;
	}
	
	public int getCols() {
		return _cols;
	}
	
	public void setBands(int bands) {
		_bands = bands;
	}
	
	public int getBands() {
		return _bands;
	}
	
	public void setCRS(String crs) {
		_crs = crs;
	}
	
	public String getCRS() {
		return _crs;
	}
	
}
