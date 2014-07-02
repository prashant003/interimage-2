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

package br.puc_rio.ele.lvc.interimage.geometry.udf.kml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.UUID;

/**
 * A UDF that parses a KML placemark.<br>
 * Note: The UDF creates an attribute in the output properties map with the name 'kml_type' and one of these values: 'Point', 'LineString' or 'Polygon'.<br><br>
 * Example:<br>
 * 		A = load 'mydata' using XMLLoader('placemark') as (placemark:chararray);<br>
 * 		B = foreach A generate KMLPlacemark(placemark);<br>
 * @author Rodrigo Ferreira
 *
 *TODO: Support 3D data; support tiles
 */
public class KMLPlacemark extends EvalFunc<Tuple> {
		
	private final GeometryParser _geometryParser = new GeometryParser();
	
	private DocumentBuilderFactory _dbFactory;
	private DocumentBuilder _dBuilder;
	
	public KMLPlacemark() throws ParserConfigurationException {
		_dbFactory = DocumentBuilderFactory.newInstance();
		_dBuilder = _dbFactory.newDocumentBuilder();
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a KML placemark
     * @exception java.io.IOException
     * @return placemark as tuple (geometry, data, properties)
     */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {
			
			String placemark = DataType.toString(input.get(0));
			
			ByteArrayInputStream in = new ByteArrayInputStream(placemark.getBytes());
		    Document doc = _dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    if (rootElement.getNodeName() != "Placemark")
		    	throw new Exception("Passed element must be <Placemark>");
			
		    NodeList extendedData = rootElement.getElementsByTagName("ExtendedData");
		    
		    Map<String, Object> properties = new HashMap<String, Object>();
		    
		    if (extendedData.getLength() > 0) {	//has attributes
		    	NodeList data = ((Element)extendedData).getElementsByTagName("Data");
		    	
		    	for (int i = 0; i < data.getLength(); i++) {
			    	Node datum = data.item(i);
			    	if (datum.getNodeType() == Node.ELEMENT_NODE) {
			        	String name = ((Element)datum).getAttribute("name");

			        	NodeList values = ((Element)datum).getElementsByTagName("value");
			        	
			        	String value = "";			        	
			        	
			        	if (values.getLength() > 0) {
			        		value = values.item(0).getTextContent();			        		
			        	}
			        	
			        	name = name.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
			        	
			        	try {
	                		//Tests if it's an integer
	                	    Integer intValue = Integer.parseInt(value);
	                	    properties.put(name, intValue);
	                	} catch (NumberFormatException nfe) {
	                	    //Not an integer. Tests if it's a double
	                		try {
	                			Double doubleValue = Double.parseDouble(value);
	                			properties.put(name, doubleValue);
	                		} catch (NumberFormatException nfe2) {
	                			//Not a double. Assuming it's a string
	                			value = value.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
	                			properties.put(name, value);
	                		}
	                	}
			        	
			        }
			    }
		    	
		    }
		    
		    Geometry geometry = null;
		    String wkt = null;
		    
		    /*Checking for Point*/
		    
		    NodeList points = rootElement.getElementsByTagName("Point");
		    NodeList lineStrings = rootElement.getElementsByTagName("LineString");
		    NodeList polygons = rootElement.getElementsByTagName("Polygon");
		    
		    if (points.getLength() > 0) {
		    	Node point = points.item(0);
		    	if (point.getNodeType() == Node.ELEMENT_NODE) {
		    		NodeList coordinates = ((Element)point).getElementsByTagName("coordinates");
		    		if (coordinates.getLength()>0) {
		    			String coords = coordinates.item(0).getTextContent();
		    			String[] c = coords.split(",");
		    			wkt = "POINT (" + c[0].trim() + " " + c[1].trim() + ")";
		    			geometry = _geometryParser.parseGeometry(wkt);
		    			properties.put("kml_type","Point");
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
		    			
		    			geometry = _geometryParser.parseGeometry(wkt);
		    			properties.put("kml_type","LineString");
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
		    		
		    		geometry = _geometryParser.parseGeometry(wkt);
		    		
		    		properties.put("kml_type","Polygon");
		    		
		    	}
		    	
		    }
		    
		    if (geometry == null) {
		    	//throw new Exception("Geometry not supported or not found");
		    	return null;
		    }
		    
		    if (!geometry.isValid())
		    	throw new Exception("Geometry not valid");
		    
		    /*Computes object id as a hash*/
		    properties.put("IIUUID",new UUID(null).random());
		    
		    if (!properties.containsKey("class"))	//preserves input class, if any
		    	properties.put("class","None");
		    		    
		    Map<String, String> data = new HashMap<String, String>();	//empty data map
		    
		    Tuple tuple = TupleFactory.getInstance().newTuple(3);
		    tuple.set(0, new DataByteArray(new WKBWriter().write(geometry)));
		    tuple.set(1, data);
		    tuple.set(2, properties);
		    
		    return tuple;
		    			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {

		try {
		
			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			list.add(new Schema.FieldSchema("geometry", DataType.BYTEARRAY));
			list.add(new Schema.FieldSchema("data", DataType.MAP));
			list.add(new Schema.FieldSchema("properties", DataType.MAP));
						
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			return new Schema(ts);
			
		} catch (Exception e) {
			return null;
		}
		
    }
	
}

