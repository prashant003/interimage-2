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

package br.puc_rio.ele.lvc.interimage.core.semanticnetwork;

import java.io.FileInputStream;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A class that holds the information about a semantic network. 
 * @author Rodrigo Ferreira
 */
public class SemanticNetwork {

	private Node _root;
	
	public void setRootNode(Node node) {
		_root = node;
	}
	
	public Node getRootNode() {
		return _root;
	}
	
	private void processNode(Node node, NodeList children) {
		
		if (node == null)
			return;
		
		if (children == null) {
			return;
		}
		
    	if (children.getLength() > 0) {
	    	
    		for (int k = 0; k < children.getLength(); k++) {
	    		    
    			org.w3c.dom.Node semNode = children.item(k);
    			    			
    			if (semNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
    			
			    	Element n = (Element)semNode;	
			    		    	
			    	Node sNode = new Node();
			    	sNode.setParentNode(node);
			    	sNode.setClassName(n.getAttribute("class"));
			    				    	 
			    	node.addChildNode(sNode);
			    	
			    	processNode(sNode, n.getChildNodes());
			    	
    			}
		    	
    		}
	    	    		
	    }
		
	}
	
	public void readOldFile(String url) {
	
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No semantic network file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No semantic network file specified");
	        	}
	        }
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();

	    	_root = new Node();
	    	_root.setParentNode(null);
	    	_root.setClassName(rootElement.getAttribute("class"));
	    	processNode(_root, rootElement.getChildNodes());		    	
		    		    
		} catch (Exception e) {
			System.err.println("Failed to read semantic network file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	private int countNodes(Node node) {
		int count = node.getChildren().size();
		for (int k=0; k<node.getChildren().size(); k++) {
			count = count + countNodes(node.getChildren().get(k));
		}
		return count;
	}
	
	public int size() {		
		return countNodes(_root) + 1;		
	}
	
}
