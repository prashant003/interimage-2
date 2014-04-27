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

package br.puc_rio.ele.lvc.interimage.datamining;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A class that holds the information about a fuzzy set.
 * @author Rodrigo Ferreira
 *
 */
@SuppressWarnings("serial")
public class FuzzySet implements Serializable {

	private String _name;
	private List<Map<String,Double>> _points;
	
	public void setName(String name) { _name = name; }
	
	public String getName() { return _name; }
	
	public void setPoints(List<Map<String,Double>> points) { _points = points; }
	
	public List<Map<String,Double>> getPoints() { return _points; }
		
}
