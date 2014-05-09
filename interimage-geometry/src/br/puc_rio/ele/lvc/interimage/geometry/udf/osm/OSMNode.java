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

package br.puc_rio.ele.lvc.interimage.geometry.udf.osm;

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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A UDF that parses an OSM node.<br><br>
 * Example:<br>
 * 		A = load 'mydata' using XMLLoader('node') as (node:chararray);<br>
 * 		B = foreach A generate OSMNode(node);<br>
 * @author Rodrigo Ferreira
 *
 */
public class OSMNode extends EvalFunc<Tuple> {
		
	private DocumentBuilderFactory _dbFactory;
	private DocumentBuilder _dBuilder;
	
	public OSMNode() throws ParserConfigurationException {
		_dbFactory = DocumentBuilderFactory.newInstance();
		_dBuilder = _dbFactory.newDocumentBuilder();
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have an OSM node
     * @exception java.io.IOException
     * @return node as tuple
     */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {
			
			String node = DataType.toString(input.get(0));
			
			ByteArrayInputStream in = new ByteArrayInputStream(node.getBytes());
		    Document doc = _dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    if (rootElement.getNodeName() != "node")
		    	throw new Exception("Passed element must be <node>");
			
		    Long id = rootElement.getAttribute("id").isEmpty() ? null : Long.parseLong(rootElement.getAttribute("id"));
		    Double lat = rootElement.getAttribute("lat").isEmpty() ? null : Double.parseDouble(rootElement.getAttribute("lat"));
		    Double lon = rootElement.getAttribute("lon").isEmpty() ? null : Double.parseDouble(rootElement.getAttribute("lon"));
		    String user = rootElement.getAttribute("user");
		    Long uid = rootElement.getAttribute("uid").isEmpty() ? null : Long.parseLong(rootElement.getAttribute("uid"));
		    Boolean visible = rootElement.getAttribute("visible").isEmpty() ? null : Boolean.parseBoolean(rootElement.getAttribute("visible"));
		    Integer version = rootElement.getAttribute("version").isEmpty() ? null : Integer.parseInt(rootElement.getAttribute("version"));
		    Integer changeset = rootElement.getAttribute("changeset").isEmpty() ? null: Integer.parseInt(rootElement.getAttribute("changeset"));
		    String timestamp = rootElement.getAttribute("timestamp");
		    
		    NodeList tags = rootElement.getElementsByTagName("tag");
		    Map<String, String> tagsMap = new HashMap<String, String>();
		    
		    for (int i = 0; i < tags.getLength(); i++) {
		    	Node tag = tags.item(i);
		    	if (tag.getNodeType() == Node.ELEMENT_NODE) {
		        	String key = ((Element)tag).getAttribute("k");
		        	String value = ((Element)tag).getAttribute("v");
		        	key = key.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
		        	value = value.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
		        	tagsMap.put(key, value);
		        }
		    }
		    
		    Tuple tuple = TupleFactory.getInstance().newTuple(10);
		    tuple.set(0, id);
		    tuple.set(1, lat);
		    tuple.set(2, lon);
		    tuple.set(3, user);
		    tuple.set(4, uid);
		    tuple.set(5, visible);
		    tuple.set(6, version);
		    tuple.set(7, changeset);
		    tuple.set(8, timestamp);
		    tuple.set(9, tagsMap);
		    
		    return tuple;
		    			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {

		try {
		
			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			list.add(new Schema.FieldSchema("id", DataType.LONG));
			list.add(new Schema.FieldSchema("lat", DataType.DOUBLE));
			list.add(new Schema.FieldSchema("long", DataType.DOUBLE));
			list.add(new Schema.FieldSchema("user", DataType.CHARARRAY));
			list.add(new Schema.FieldSchema("uid", DataType.LONG));
			list.add(new Schema.FieldSchema("visible", DataType.BOOLEAN));
			list.add(new Schema.FieldSchema("version", DataType.INTEGER));
			list.add(new Schema.FieldSchema("changeset", DataType.INTEGER));
			list.add(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));
			
			Schema tagSchema = new Schema();
			tagSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));
			FieldSchema temp = new Schema.FieldSchema("tags", tagSchema);
			temp.type = DataType.MAP;
			list.add(temp);
						
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			return new Schema(ts);
			
		} catch (Exception e) {
			return null;
		}
		
    }
	
}

