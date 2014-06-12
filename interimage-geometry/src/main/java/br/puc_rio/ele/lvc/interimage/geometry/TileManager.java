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

package br.puc_rio.ele.lvc.interimage.geometry;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that manages the tiles.
 * @author Rodrigo Ferreira
 *
 */
public class TileManager {

	private double _tileSize; 
	private int _numTilesX;
	private int _numTilesY;
	private double[] _worldBBox;
	private List<Tile> _tiles;
	private int _levels;
	
	public TileManager(double size) {
		setSize(size);
		_tiles = new ArrayList<Tile>();
	}
	
	public void setSize(double size) {
		_tileSize = size;
		
		WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
        webMercator.setDatum("WGS84");
		
		_numTilesX = (int)Math.ceil((webMercator.getGeoEast()-webMercator.getGeoWest()) / _tileSize);
		_numTilesY = (int)Math.ceil((webMercator.getGeoNorth()-webMercator.getGeoSouth()) / _tileSize);
		
		/*Computing a squared tile coordinate system*/
		if (_numTilesX >= _numTilesY) {
			_numTilesY = _numTilesX;
		} else
			_numTilesX = _numTilesY;
		
		/*Number of levels: ceil(log2(num_tiles_x))*/
		_levels = (int)Math.ceil(Math.log(_numTilesX) / Math.log(2));
		
		_worldBBox = new double[] {webMercator.getGeoWest(), webMercator.getGeoSouth(), webMercator.getGeoEast(), webMercator.getGeoNorth()}; 
		
	}
	
	/*public List<Long> getTiles(double[] bbox) {
        
        int tileMinX = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
        int tileMinY = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
        int tileMaxX = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
        int tileMaxY = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
                
        ArrayList<Long> list = new ArrayList<Long>();
        
        for (int j=tileMinY; j<=tileMaxY; j++) {
        	for (int i=tileMinX; i<=tileMaxX; i++) {
        		list.add(((long)j)*_numTilesX+i+1);
        	}
        }
        
        return list;
        
	}*/
	
	public String encodeCoordinates(int i, int j, int level) {
		
		if (level>=_levels)
			return "";
		
		int modx = i % 2;
		int mody = j % 2;
				
		String code = new String();
		
		if ((modx==0) && (mody==0)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + "a";
		} else if ((modx==0) && (mody==1)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + "b";
		} else if ((modx==1) && (mody==0)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + "c";
		} else if ((modx==1) && (mody==1)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + "d";
		}
				
		return code;
		
	}
	
	public List<String> getTiles(double[] bbox) {
        
        int tileMinX = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
        int tileMinY = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
        int tileMaxX = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
        int tileMaxY = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
                
        ArrayList<String> list = new ArrayList<String>();
        
        for (int j=tileMinY; j<=tileMaxY; j++) {
        	for (int i=tileMinX; i<=tileMaxX; i++) {        		
        		String code = encodeCoordinates(i,j,0);
        		list.add(code);
        	}
        }
        
        return list;
        
	}
	
	public int[] getTileCoordinates(double[] bbox) {
		int[] tileCoords = new int[4];
		tileCoords[0] = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
		tileCoords[1] = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
		tileCoords[2] = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
		tileCoords[3] = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
		
		return tileCoords;
	}
	
	public int getNumTilesX() {
		return _numTilesX;
	}
	
	public int getNumTilesY() {
		return _numTilesY;
	}
	
	public double getTileSize() {
		return _tileSize;
	}
	
	public void setTiles(double[] geoBBox) {
				
		int[] tileCoords = getTileCoordinates(geoBBox);
				
		for (int j=tileCoords[1]; j<=tileCoords[3]; j++) {
			for (int i=tileCoords[0]; i<=tileCoords[2]; i++) {
				Tile tile = new Tile();
				tile.setId(((long)j)*_numTilesX+i+1);
				
				/*Computing code*/
				String code = encodeCoordinates(i,j,0);
				tile.setCode(code);
				
				double geoX = i*_tileSize + _worldBBox[0];
				double geoY = j*_tileSize + _worldBBox[1];
				
				tile.setGeometry(String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, geoX + _tileSize, geoY, geoX + _tileSize, geoY + _tileSize, geoX, geoY + _tileSize, geoX, geoY));
								
				_tiles.add(tile);
			}
		}
		
	}
	
	public List<Tile> getTiles() {
		return _tiles;
	}
	
}
