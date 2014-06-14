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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
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
 * A UDF that parses an OSM way.<br><br>
 * Example:<br>
 * 		A = load 'mydata' using XMLLoader('way') as (way:chararray);<br>
 * 		B = foreach A generate OSMWay(way);<br>
 * @author Rodrigo Ferreira
 *
 */
public class OSMWay extends EvalFunc<Tuple> {
		
	private DocumentBuilderFactory _dbFactory;
	private DocumentBuilder _dBuilder;
	
	public OSMWay() throws ParserConfigurationException {
		_dbFactory = DocumentBuilderFactory.newInstance();
		_dBuilder = _dbFactory.newDocumentBuilder();
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have an OSM way
     * @exception java.io.IOException
     * @return way as tuple
     */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {
			
			String way = DataType.toString(input.get(0));
			
			ByteArrayInputStream in = new ByteArrayInputStream(way.getBytes());
		    Document doc = _dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
		    
		    if (rootElement.getNodeName() != "way")
		    	throw new Exception("Passed element must be <way>");
			
		    Long id = rootElement.getAttribute("id").isEmpty() ? null : Long.parseLong(rootElement.getAttribute("id"));
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
		    
		    NodeList nodes = rootElement.getElementsByTagName("nd");
		    DataBag nodesBag = BagFactory.getInstance().newDefaultBag();
		    
		    for (int i = 0; i < nodes.getLength(); i++) {
		    	Node node = nodes.item(i);
		    	if (node.getNodeType() == Node.ELEMENT_NODE) {
		    		int node_pos = (int)(nodesBag.size() + 1);
		    		long node_id = Long.parseLong(((Element)node).getAttribute("ref"));
		    		Tuple nodeTuple = TupleFactory.getInstance().newTuple(2);
		    		nodeTuple.set(0, node_pos);
		    		nodeTuple.set(1, node_id);
		    		nodesBag.add(nodeTuple);
		    	}
		    }
		    
		    Tuple tuple = TupleFactory.getInstance().newTuple(9);
		    tuple.set(0, id);
		    tuple.set(1, user);
		    tuple.set(2, uid);
		    tuple.set(3, visible);
		    tuple.set(4, version);
		    tuple.set(5, changeset);
		    tuple.set(6, timestamp);
		    tuple.set(7, tagsMap);
		    tuple.set(8, nodesBag);
		    
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
		      
			Schema nodeSchema = new Schema();
			nodeSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			nodeSchema.add(new Schema.FieldSchema("node_id", DataType.LONG));
			temp = new Schema.FieldSchema("nodes", nodeSchema);
			temp.type = DataType.BAG;
			list.add(temp);
			
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			return new Schema(ts);
			
		} catch (Exception e) {
			return null;
		}
		
    }
	
}

