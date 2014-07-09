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

package br.puc_rio.ele.lvc.interimage.operators;
import com.vividsolutions.jts.geom.Polygon;

/**
 * An image object or segment for thresholding<br>
 * 
 * 
 * @author Patrick Happ
 *
 */

public class LimiarSegment {
	//TODO: make it generic? Can use a Map of attributes!!!
	private int _segmentId;			
	private String _objClass; //could be a map if it would be useful to compare between classes
	private Polygon _polygon;
		
	public LimiarSegment(int id){
		_segmentId=id;
		_objClass="UNKNOWN";
		_polygon=null;
	}
	
	public LimiarSegment(int id, String objClass){
		_segmentId=id;
		_objClass=objClass;
		_polygon=null;
	}
	
	public int getsegmentId() {
		return _segmentId;
	}

	public void setSegmentId(int segmentId) {
		this._segmentId = segmentId;
	}
	
	public String getObjClass() {
		return _objClass;
	}

	public void setObjClass(String objClass) {
		this._objClass = objClass;
	}
	
	public Polygon getPolygon() {
		return _polygon;
	}

	public void setPolygon(Polygon poly) {
		this._polygon = poly;
	}
}


