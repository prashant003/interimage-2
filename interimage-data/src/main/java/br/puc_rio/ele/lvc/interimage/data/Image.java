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

	public static double imgToGeoX(double x, int cols, double imgGeo[]) {
	    return ((x+0.5) * ((imgGeo[2]-imgGeo[0]) / cols)) + imgGeo[0];
	}
	
	public static double imgToGeoY(double y, int rows, double imgGeo[]) {
	    return ((y+0.5) * ((imgGeo[1]-imgGeo[3]) / rows)) + imgGeo[3];
	}
	
	public static double geoToImgX(double geoX, int cols, double imgGeo[]) {
		return ((geoX-imgGeo[0]) / (imgGeo[2]-imgGeo[0])) * cols;
	}
	
	public static double geoToImgY(double geoY, int rows, double imgGeo[]) {
		return ((geoY-imgGeo[3]) / (imgGeo[1]-imgGeo[3])) * rows;
	}

	public static int[] imgBBox(double tileGeo[], double imgGeo[], int imgSize[]) {
		int[] tileBBox = new int[4];
		tileBBox[0] = (int)Math.floor(geoToImgX(tileGeo[0],imgSize[0],imgGeo) + 0.5);
		tileBBox[2] = (int)Math.floor(geoToImgX(tileGeo[2],imgSize[0],imgGeo) - 0.5);
	    tileBBox[1] = (int)Math.floor(geoToImgY(tileGeo[1],imgSize[1],imgGeo) - 0.5);	    
	    tileBBox[3] = (int)Math.floor(geoToImgY(tileGeo[3],imgSize[1],imgGeo) + 0.5);
	    return tileBBox;
	}

	public static double[] geoBBox(int tile[], double imgGeo[], int imgSize[]) {
		double[] geoBBox = new double[4];
		geoBBox[0] = imgToGeoX(tile[0] - 0.5, imgSize[0], imgGeo);
		geoBBox[2] = imgToGeoX(tile[2] + 0.5, imgSize[0], imgGeo);
		geoBBox[1] = imgToGeoY(tile[1] + 0.5, imgSize[1], imgGeo);
		geoBBox[3] = imgToGeoY(tile[3] - 0.5, imgSize[1], imgGeo);
	    return geoBBox;
	}
	
}
