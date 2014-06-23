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
			    		
			    		map.put("alias",udfElem.getAttribute("alias"));
			    		map.put("import",udfElem.getAttribute("import"));
			    		map.put("isSpectral",udfElem.getAttribute("isSpectral"));
			    		
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
