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

package br.puc_rio.ele.lvc.interimage.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private String _crs;
	
	/* Z-order
	 *  ----------------
	 * |	1	|	3	|
	 *  ----------------
	 * |	0	|	2	|
	 *  ----------------
	 * */
		
	private String[] _directions = new String[] {"l", "m", "s", "g"}; //libeccio, maestro, greco, scirocco
	//private String[] _directions = new String[] {"w", "x", "y", "z"};	//
	//private String[] _directions = new String[] {"a", "b", "c", "d"};
	
	Map<String,Integer> _directionsMap = new HashMap<String,Integer>();
	
	Map<String,List<String>> _borders = new HashMap<String,List<String>>();
	Map<String,String[]> _neighbors = new HashMap<String,String[]>();
	
	public TileManager(double size, String crs) {
		setSize(size, crs);
		_tiles = new ArrayList<Tile>();
		
		_directionsMap.put(_directions[0],0);
		_directionsMap.put(_directions[1],1);
		_directionsMap.put(_directions[2],2);
		_directionsMap.put(_directions[3],3);
		
		String[] vertical = new String[] {_directions[1], _directions[0], _directions[3], _directions[2]};
		String[] diagonal = new String[] {_directions[3], _directions[2], _directions[1], _directions[0]};
		String[] horizontal = new String[] {_directions[2], _directions[3], _directions[0], _directions[1]};
		
		List<String> nBorder = new ArrayList<String>();
		nBorder.add(_directions[1]);
		nBorder.add(_directions[3]);
		_neighbors.put("N", vertical);
		_borders.put("N", nBorder);		
				
		List<String> neBorder = new ArrayList<String>();
		neBorder.add(_directions[1]);
		neBorder.add(_directions[3]);
		neBorder.add(_directions[2]);
		_neighbors.put("NE", diagonal);
		_borders.put("NE", neBorder);
		
		List<String> eBorder = new ArrayList<String>();
		eBorder.add(_directions[2]);
		eBorder.add(_directions[3]);
		_neighbors.put("E", horizontal);
		_borders.put("E", eBorder);
		
		List<String> seBorder = new ArrayList<String>();
		seBorder.add(_directions[0]);
		seBorder.add(_directions[2]);
		seBorder.add(_directions[3]);
		_neighbors.put("SE", diagonal);
		_borders.put("SE", seBorder);
		
		List<String> sBorder = new ArrayList<String>();
		sBorder.add(_directions[0]);
		sBorder.add(_directions[2]);
		_neighbors.put("S", vertical);
		_borders.put("S", sBorder);
		
		List<String> swBorder = new ArrayList<String>();
		swBorder.add(_directions[0]);
		swBorder.add(_directions[1]);
		swBorder.add(_directions[2]);
		_neighbors.put("SW", diagonal);
		_borders.put("SW", swBorder);
		
		List<String> wBorder = new ArrayList<String>();
		wBorder.add(_directions[0]);
		wBorder.add(_directions[1]);
		_neighbors.put("W", horizontal);
		_borders.put("W", wBorder);
		
		List<String> nwBorder = new ArrayList<String>();
		nwBorder.add(_directions[0]);
		nwBorder.add(_directions[1]);
		nwBorder.add(_directions[3]);
		_neighbors.put("NW", diagonal);
		_borders.put("NW", nwBorder);
		
	}
	
	private void setSize(double size, String crs) {
		_tileSize = size;
		
		_crs = crs;
		
		double[] bounds = new CRS().getBounds(_crs); 
		
		_numTilesX = (int)Math.ceil((bounds[2]-bounds[0]) / _tileSize);
		_numTilesY = (int)Math.ceil((bounds[3]-bounds[1]) / _tileSize);
		
		/*It seems that the dimensions don't have to be equal nor even*/
		
		_levels = (int)Math.ceil(Math.log((double)Math.max(_numTilesX,_numTilesY)) / Math.log(2));
		
		_worldBBox = new double[] {bounds[0], bounds[1], bounds[2], bounds[3]}; 
				
	}
	
	public String encodeCoordinates(int i, int j, int level) {
		
		if (level>=_levels)
			return "";
		
		int modx = i % 2;
		int mody = j % 2;
				
		String code = new String();
		
		if ((modx==0) && (mody==0)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + _directions[0];
		} else if ((modx==0) && (mody==1)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + _directions[1];
		} else if ((modx==1) && (mody==0)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + _directions[2];
		} else if ((modx==1) && (mody==1)) {
			code = encodeCoordinates((int)Math.floor(((double)i)/2), (int)Math.floor(((double)j)/2), level+1) + _directions[3];
		}
				
		return code;
		
	}
	
	public List<String> getTiles(double[] bbox) {
        
        /*int tileMinX = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
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
        
        return list;*/
		
		int tileMinX = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
        int tileMinY = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
        int tileMaxX = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
        int tileMaxY = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
                
        ArrayList<String> list = new ArrayList<String>();
        
        for (int j=tileMinY; j<=tileMaxY; j++) {
        	for (int i=tileMinX; i<=tileMaxX; i++) {
        		long id = ((long)j)*_numTilesX+i+1;
        		list.add("T" + id);
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
				long id = ((long)j)*_numTilesX+i+1;
				tile.setId(id);
				
				/*Computing code*/
				//String code = encodeCoordinates(i,j,0);
				//tile.setCode(code);
				tile.setCode("T" + id);
				
				double geoX = i*_tileSize + _worldBBox[0];
				double geoY = j*_tileSize + _worldBBox[1];
				
				tile.setGeometry(String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, geoX + _tileSize, geoY, geoX + _tileSize, geoY + _tileSize, geoX, geoY + _tileSize, geoX, geoY));
								
				_tiles.add(tile);
			}
		}
		
	}
	
	/*public Geometry getTileGeometry(String tile) {
		
		Geometry geometry = null;
		
		try {
		
			long idx = Long.parseLong(tile.substring(1)) - 1;
			
			int i = (int)(idx % _numTilesX);
			int j = (int)(idx / _numTilesX);
			
			double geoX = i*_tileSize + _worldBBox[0];
			double geoY = j*_tileSize + _worldBBox[1];
			
			String geomStr = String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, geoX + _tileSize, geoY, geoX + _tileSize, geoY + _tileSize, geoX, geoY + _tileSize, geoX, geoY);
			
			geometry = new WKTReader().read(geomStr);
			
		} catch (Exception e) {
			System.out.println("Failed to compute geometry from id: " + e.getMessage());
		}
		
		return geometry;
		
	}*/
	
	private String getUpperDirection(String current, String direction) {
		
		if (_directionsMap.get(current)==0) {
			if (direction.equals("SE"))
				return "S";
			else if (direction.equals("NW"))
				return "W";			
		}
		
		if (_directionsMap.get(current)==1) {
			if (direction.equals("NE"))
				return "N";
			else if (direction.equals("SW"))
				return "W";			
		}
		
		if (_directionsMap.get(current)==2) {
			if (direction.equals("NE"))
				return "E";
			else if (direction.equals("SW"))
				return "S";			
		}
		
		if (_directionsMap.get(current)==3) {
			if (direction.equals("SE"))
				return "E";
			else if (direction.equals("NW"))
				return "N";			
		}
		
		return direction;
		
	}
	
	private String getUpperLevelNeighbor(String current, String upper, String direction) {

		if (_borders.get(direction).contains(current)) {
			return _neighbors.get(getUpperDirection(current, direction))[_directionsMap.get(upper)];
		}
				
		return upper;
	}
		
	public String getNeighourTiles(String code) {
				
		String root = code.substring(0,code.length()-2);
		String upperLevel = code.substring(code.length()-2,code.length()-1);
		String currentLevel = code.substring(code.length()-1);
				
		String neighbours = new String();
		
		//N
		neighbours = root + getUpperLevelNeighbor(currentLevel, upperLevel, "N") + _neighbors.get("N")[_directionsMap.get(currentLevel)];
		
		//NE
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "NE") + _neighbors.get("NE")[_directionsMap.get(currentLevel)];
		
		//E
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "E") + _neighbors.get("E")[_directionsMap.get(currentLevel)];
		
		//SE
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "SE") + _neighbors.get("SE")[_directionsMap.get(currentLevel)];
		
		//S
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "S") + _neighbors.get("S")[_directionsMap.get(currentLevel)];
		
		//SW
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "SW") + _neighbors.get("SW")[_directionsMap.get(currentLevel)];
		
		//W
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "W") + _neighbors.get("W")[_directionsMap.get(currentLevel)];
		
		//NW
		neighbours += "," + root + getUpperLevelNeighbor(currentLevel, upperLevel, "NW") + _neighbors.get("NW")[_directionsMap.get(currentLevel)];
		
		return neighbours;
	}
	
	public String getCRS() {
		return _crs;
	}
	
	public List<Tile> getTiles() {
		return _tiles;
	}
	
	public double[] getWorldBBox() {
		return _worldBBox;
	}
	
}
