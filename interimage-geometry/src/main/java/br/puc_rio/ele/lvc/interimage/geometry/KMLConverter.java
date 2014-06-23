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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Converts between KML format and InterIMAGE formats.<br>
 * @author Rodrigo Ferreira
 * TODO: Add tile info to the polygons
 */
public class KMLConverter {
	
	/**
	 * Converts from KML to JSON format.<br>
	 * @param input KML path<br>
	 * output JSON file path
	 */	
	public static void KMLToJSON(String kml, String json, List<String> names, boolean keep) {
	
	}
	
	/**
	 * Converts from JSON to KML format.<br>
	 * @param input JSON path<br>
	 * output KML file path
	 */	
	public static void JSONToKML(String json, String kml, List<String> names, boolean keep) {
	
	}
	
	/**
	 * Converts from KML to WKT format.<br>
	 * @param input KML path<br>
	 * output WKT file path
	 */	
	public static void KMLToWKT(String kml, String wktFile) {
	
		try {
			
			/*Processing input parameters*/
			if (wktFile == null) {
	            throw new Exception("No WKT file specified");
	        } else {
	        	if (wktFile.isEmpty()) {
	        		throw new Exception("No WKT file specified");
	        	}
	        }
	
			if (kml == null) {
	            throw new Exception("No KML file specified");
	        } else {
	        	if (kml.isEmpty()) {
	        		throw new Exception("No KML file specified");
	        	}
	        }
			
			GeometryParser geometryParser = new GeometryParser();
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(kml);
	        
			OutputStream out = new FileOutputStream(wktFile);
			
		    Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
			
		    NodeList placemarks = rootElement.getElementsByTagName("Placemark");
		    
		    if (placemarks.getLength() > 0) {
		    	
		    	for (int k = 0; k < placemarks.getLength(); k++) {
			    	Node placemark = placemarks.item(k);
			    	
				    Geometry geometry = null;
				    String wkt = null;
				    
				    /*Checking for Point*/
				    
				    NodeList points = ((Element)placemark).getElementsByTagName("Point");
				    NodeList lineStrings = ((Element)placemark).getElementsByTagName("LineString");
				    NodeList polygons = ((Element)placemark).getElementsByTagName("Polygon");
				    
				    if (points.getLength() > 0) {
				    	Node point = points.item(0);
				    	if (point.getNodeType() == Node.ELEMENT_NODE) {
				    		NodeList coordinates = ((Element)point).getElementsByTagName("coordinates");
				    		if (coordinates.getLength()>0) {
				    			String coords = coordinates.item(0).getTextContent();
				    			String[] c = coords.split(",");
				    			wkt = "POINT (" + c[0].trim() + " " + c[1].trim() + ")";
				    			geometry = geometryParser.parseGeometry(wkt);				    			
				    		}
				    		
				    	}
				    } else if (lineStrings.getLength() > 0) {
				    
					    /*Checking for LineString*/
					    				    	
				    	Node lineString = lineStrings.item(0);
				    	if (lineString.getNodeType() == Node.ELEMENT_NODE) {
				    		NodeList coordinates = ((Element)lineString).getElementsByTagName("coordinates");
				    		if (coordinates.getLength()>0) {
				    			String coords = coordinates.item(0).getTextContent();
				    			String[] c = coords.split("\\r?\\n");
				    			
				    			/*Considering only 2D for now*/
				    			wkt = "LINESTRING (";
				    							    			
				    			boolean first = true;
				    			
				    			for (int w=0; w<c.length; w++) {
				    				
				    				if (c[w].trim().isEmpty())
				    					continue;
				    				
				    				String[] c1 = c[w].split(",");
				    				
				    				if (first) {
				    					first = false;
				    				} else {
				    					wkt = wkt + ", ";
				    				}
				    				wkt = wkt + c1[0].trim() + " " + c1[1].trim();
				    			}
				    							    			
				    			wkt = wkt + ")";
				    			
				    			geometry = geometryParser.parseGeometry(wkt);				    			
				    		}
				    		
				    	}
				        
				    } else if (polygons.getLength() > 0) {
					
				    	/*Checking for Polygon*/
					    
				    	Node polygon = polygons.item(0);
				    	if (polygon.getNodeType() == Node.ELEMENT_NODE) {
				    		NodeList obis = ((Element)polygon).getElementsByTagName("outerBoundaryIs");
				    		
				    		wkt = "POLYGON (";
				    		
				    		if (obis.getLength()>0) {
				    			
				    			NodeList coordinates = ((Element)obis.item(0)).getElementsByTagName("coordinates");
				    			
				    			if (coordinates.getLength()>0) {
					    			String coords = coordinates.item(0).getTextContent();
					    			String[] c = coords.split("\\r?\\n");
					    			
					    			/*Considering only 2D for now*/
					    			wkt = wkt + "(";
					    			
					    			boolean first = true;
					    			
					    			for (int w=0; w<c.length; w++) {
					    				
					    				if (c[w].trim().isEmpty())
					    					continue;
					    				
					    				String[] c1 = c[w].split(",");
					    				
					    				if (first) {
					    					first = false;
					    				} else {
					    					wkt = wkt + ", ";
					    				}
					    				wkt = wkt + c1[0].trim() + " " + c1[1].trim();
					    			}
					    			
					    			wkt = wkt + ")";
					    			
					    		}
				    		}
				    		
				    		NodeList ibis = ((Element)polygon).getElementsByTagName("innerBoundaryIs");
				    		
				    		if (ibis.getLength()>0) {
				    			
				    			NodeList ls = ((Element)ibis.item(0)).getElementsByTagName("LinearRing");
				    			
				    			for (int j=0; j<ls.getLength(); j++) {
				    			
				    				wkt = wkt + ", ";
					    			
					    			NodeList coordinates = ((Element)ls.item(j)).getElementsByTagName("coordinates");
					    			
					    			if (coordinates.getLength()>0) {
						    			String coords = coordinates.item(0).getTextContent();
						    			String[] c = coords.split("\\r?\\n");
						    			
						    			/*Considering only 2D for now*/
						    			wkt = wkt + "(";
						    			
						    			boolean first = true;
						    			
						    			for (int w=0; w<c.length; w++) {
						    				
						    				if (c[w].trim().isEmpty())
						    					continue;
						    				
						    				String[] c1 = c[w].split(",");
						    				
						    				if (first) {
						    					first = false;
						    				} else {
						    					wkt = wkt + ", ";
						    				}
						    				wkt = wkt + c1[0].trim() + " " + c1[1].trim();
						    			}
						    			
						    			wkt = wkt + ")";
						    			
						    		}
					    		}
				    			
				    		}
				    		
				    		wkt = wkt + ")";
				    						    		
				    		geometry = geometryParser.parseGeometry(wkt);
				    		
				    	}
				    	
				    }
				    
				    if (geometry == null) {
				    	//out.close();
				    	//throw new Exception("Geometry not supported or not found");
				    	continue;
				    }
				    
				    if (!geometry.isValid()) {
				    	out.close();
				    	throw new Exception("Geometry not valid");				    	
				    }
			    	
				    wkt = wkt + "\n";
				    
				    out.write(wkt.getBytes());
				    
		    	}
		    	
		    }
		    
		    out.close();
		    
		} catch (Exception e) {
			System.err.println("Failed to parse KML file; error - " + e.getMessage());
		}
		
	}
	
	/**
	 * Converts from WKT to KML format.<br>
	 * @param input WKT path<br>
	 * output KML file path
	 */	
	public static void WKTToKML(String wkt, String kml) {
	
		try {
			
			/*Processing input parameters*/
			if (wkt == null) {
	            throw new Exception("No WKT file specified");
	        } else {
	        	if (wkt.isEmpty()) {
	        		throw new Exception("No WKT file specified");
	        	}
	        }
	
			if (kml == null) {
	            throw new Exception("No KML file specified");
	        } else {
	        	if (kml.isEmpty()) {
	        		throw new Exception("No KML file specified");
	        	}
	        }
			
		} catch (Exception e) {
			System.err.println("Failed to create KML file; error - " + e.getMessage());
		}
		
	}
	
}
