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

package br.puc_rio.ele.lvc.interimage.core.udf;

import java.io.File;
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
 * A class that holds the UDFs. 
 * @author Rodrigo Ferreira
 */
public class UDFSet {

	Map<String, Map<String, Object>> _udfs;
	
	public UDFSet() {
		_udfs = new HashMap<String, Map<String, Object>>();
	}
	
	private void readUDFFile(String url) {
		
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No udf file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No udf file specified");
	        	}
	        }
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    NodeList udfs = rootElement.getElementsByTagName("udf");
		    
		    if (udfs.getLength() > 0) {
		    	
		    	for (int i = 0; i < udfs.getLength(); i++) {
		    		
		    		Node udf = udfs.item(i);
		    		
			    	if (udf.getNodeType() == Node.ELEMENT_NODE) {
			    		
			    		Element udfElem = ((Element)udf);
			    		
			    		Map<String, Object> map = new HashMap<String, Object>();
			    		
			    		String name = udfElem.getAttribute("name");
			    		
			    		String oldName = udfElem.getAttribute("oldName");
			    		
			    		map.put("alias",udfElem.getAttribute("alias"));
			    		map.put("import",udfElem.getAttribute("import"));
			    		map.put("isSpectral",udfElem.getAttribute("isSpectral"));
			    		map.put("lazyDefinition",udfElem.getAttribute("lazyDefinition"));
			    		
			    		NodeList params = udfElem.getElementsByTagName("param");
			    			
		    			List<String> list = new ArrayList<String>();
		    			
		    			for (int j = 0; j < params.getLength(); j++) {		    				
		    				list.add(params.item(j).getTextContent());		    				
		    			}
			    			
			    		map.put("params", list);
			    					    	
			    		NodeList macros = udfElem.getElementsByTagName("macro");
			    					    			
		    			List<String> list2 = new ArrayList<String>();	
		    			
		    			for (int j = 0; j < macros.getLength(); j++) {		    				
		    				list2.add(macros.item(j).getTextContent());
		    			}
		    			
			    		map.put("macros", list2);
			    		
			    		_udfs.put(name, map);
			    		
			    		if (!oldName.isEmpty())
			    			_udfs.put(oldName, map);
			    		
			    	}
		    		
		    	}
		    	
		    }
		    
		} catch (Exception e) {
			System.err.println("Failed to read udf file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void loadUDFs() {
		
		File folder = new File("udf");
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	//ignore
	        } else {
	            readUDFFile("udf/" + fileEntry.getName());
	        }
	    }
	}
	
	public Map<String, Map<String, Object>> getUDFs() {
		return _udfs;
	}
	
}
