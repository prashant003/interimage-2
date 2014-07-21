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

package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A class that holds the operators. 
 * @author Rodrigo Ferreira
 */
public class OperatorSet {

Map<String, Map<String, Object>> _operators;
	
	public OperatorSet() {
		_operators = new HashMap<String, Map<String, Object>>();
	}
	
	private void readOperatorFile(String url) {
		
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No operator file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No operator file specified");
	        	}
	        }
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    NodeList operators = rootElement.getElementsByTagName("operator");
		    
		    if (operators.getLength() > 0) {
		    	
		    	for (int i = 0; i < operators.getLength(); i++) {
		    		
		    		Node operator = operators.item(i);
		    		
			    	if (operator.getNodeType() == Node.ELEMENT_NODE) {
			    		
			    		Element operatorElem = ((Element)operator);
			    		
			    		Map<String, Object> map = new HashMap<String, Object>();
			    		
			    		String name = operatorElem.getAttribute("name");
			    		
			    		String oldName = operatorElem.getAttribute("oldName");
			    		
			    		/*map.put("alias",operatorElem.getAttribute("alias"));
			    		map.put("import",operatorElem.getAttribute("import"));
			    		map.put("isSpectral",operatorElem.getAttribute("isSpectral"));
			    		map.put("lazyDefinition",operatorElem.getAttribute("lazyDefinition"));*/
			    		
			    		NodeList inputs = operatorElem.getElementsByTagName("input");
			    			
		    			Map<String, String> map1 = new HashMap<String,String>();
		    			
		    			for (int j = 0; j < inputs.getLength(); j++) {		    				
		    				//list.add(inputs.item(j).getTextContent());
		    				
		    				Element inputElem = ((Element)inputs.item(j));
		    				
		    				map1.put("type", inputElem.getAttribute("type"));
		    				map1.put("name", inputs.item(j).getTextContent());
		    			}
			    			
			    		map.put("inputs", map1);
			    					    	
			    		NodeList outputs = operatorElem.getElementsByTagName("output");
		    			
		    			Map<String, String> map2 = new HashMap<String,String>();
		    			
		    			for (int j = 0; j < outputs.getLength(); j++) {		    				
		    				//list.add(inputs.item(j).getTextContent());
		    				
		    				Element outputElem = ((Element)outputs.item(j));
		    				
		    				map2.put("type", outputElem.getAttribute("type"));
		    				map2.put("name", outputs.item(j).getTextContent());
		    			}
			    			
			    		map.put("outputs", map2);
			    		
			    		NodeList templates = operatorElem.getElementsByTagName("template");
			    		
			    		//just one
			    		map.put("template", templates.item(0).getTextContent());
			    		
			    		_operators.put(name, map);
			    		
			    		if (!oldName.isEmpty())
			    			_operators.put(oldName, map);
			    		
			    	}
		    		
		    	}
		    	
		    }
		    
		} catch (Exception e) {
			System.err.println("Failed to read operator file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void loadOperators() {
		
		File folder = new File("operator");
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	//ignore
	        } else {
	            readOperatorFile("operator/" + fileEntry.getName());
	        }
	    }
	}
	
	public Map<String, Map<String, Object>> getOperators() {
		return _operators;
	}
	
}
