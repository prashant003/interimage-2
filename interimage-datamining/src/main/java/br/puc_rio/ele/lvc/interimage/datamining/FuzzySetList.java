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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A class that holds the information about a list of fuzzy sets.
 * @author Rodrigo Ferreira
 */
public class FuzzySetList {

	private Map<String,FuzzySet>_fuzzySets;
	private String _url;
	
	public FuzzySetList() {
		_fuzzySets = new HashMap<String,FuzzySet>();
	}
	
	public void readOldFile(String url) {
		
		try {
			
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No fuzzy sets file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No fuzzy sets file specified");
	        	}
	        }
			
			_url = url;
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();

		    NodeList fuzzySets = rootElement.getElementsByTagName("fuzzyset");
	    		    	
		    if (fuzzySets.getLength() > 0) {
		    	
		    	for (int k = 0; k < fuzzySets.getLength(); k++) {
		    		
		    		Node fuzzyNode = fuzzySets.item(k);
		    		
		    		if (fuzzyNode.getNodeType() == Node.ELEMENT_NODE) {
		    			
			    		Element fuzzySet = (Element)fuzzyNode;
			    		
				    	FuzzySet fs = new FuzzySet();
				    	String name = fuzzySet.getAttribute("name");
				    	
				    	fs.setName(name);
				    	
				    	NodeList list = fuzzySet.getChildNodes();
				    	
				    	List<Map<String,Double>> points = new ArrayList<Map<String,Double>>();
				    	
				    	if (list.getLength() > 0) {
					    	
				    		for (int u = 0; u < list.getLength(); u++) {
					    		    
				    			Node pointNode = list.item(u);
				    			    			
				    			if (pointNode.getNodeType() == Node.ELEMENT_NODE) {
				    			
							    	Element n = (Element)pointNode;	
							    				
									Map<String,Double> map = new HashMap<String,Double>();
									
									map.put("first", new Double(n.getAttribute("x")));
									map.put("second", new Double(n.getAttribute("y")));
									points.add(map);
							    	
				    			}
						    	
				    		}
					    	    		
					    }
				    	
				    	fs.setPoints(points);
				    	
				    	_fuzzySets.put(name, fs);
				    	
		    		}
		    		
		    	}
		    	
		    }		    
		    		    
		} catch (Exception e) {
			System.err.println("Failed to read fuzzy sets file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void add(String key, FuzzySet fuzzySet) {
		_fuzzySets.put(key, fuzzySet);		
	}
	
	public int size() {
		return _fuzzySets.size();
	}
	
	public Map<String,FuzzySet> getFuzzySets() {
		return _fuzzySets;
	}
	
	public String getURL() {
		return _url;
	}
	
}
