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

import java.io.Serializable;

/**
 * A class that holds the information about a tile.
 * @author Rodrigo Ferreira
 *
 */
@SuppressWarnings("serial")
public class Tile implements Serializable {

	private long _id;
	private String _code;
	private String _geometry;
		
	public void setId(long id) { _id = id; }
	
	public String getCode() { return _code; }
	
	public void setCode(String code) { _code = code; }
	
	public long getId() { return _id; }
	
	public void setGeometry(String geometry) { _geometry = geometry; }
	
	public String getGeometry() { return _geometry; }
	
}