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

import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds the information about a semantic network node. 
 * @author Rodrigo Ferreira
 */
public class Node {

	private String _className;
	private Node _parent;
	private List<Node> _children;
	
	public Node() {
		_children = new ArrayList<Node>(); 
	}
	
	public void setClassName(String c) {
		_className = c;
	}
	
	public String getClassName() {
		return _className;
	}
	
	public void addChildNode(Node child) {
		_children.add(child);
	}
	
	public void setParentNode(Node parent) {
		_parent = parent;
	}
	
	public Node getParentNode() {
		return _parent;
	}
	
	public List<Node> getChildren() {
		return _children;
	}
	
}
