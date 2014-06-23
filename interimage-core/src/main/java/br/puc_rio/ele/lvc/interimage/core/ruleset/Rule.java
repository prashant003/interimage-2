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

package br.puc_rio.ele.lvc.interimage.core.ruleset;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds the information about a rule. 
 * @author Rodrigo Ferreira
 */
public class Rule {

	private Rule _parent;
	private String _type;
	private String _label;
	private String _commented;
	private String _scope;
	private List<Rule> _children;
	
	public Rule() {
		_children = new ArrayList<Rule>(); 
	}
	
	public void setParent(Rule parent) {
		_parent = parent;
	}
	
	public Rule getParent() {
		return _parent;
	}
	
	public void setType(String type) {
		_type = type;
	}
	
	public String getType() {
		return _type;
	}
	
	public void setLabel(String label) {
		_label = label;
	}
	
	public String getLabel() {
		return _label;
	}
	
	public void setCommented(String commented) {
		_commented = commented;
	}
	
	public String getCommented() {
		return _commented;
	}
	
	public void setScope(String scope) {
		_scope = scope;
	}
	
	public String getScope() {
		return _scope;
	}
	
	public void addChildRule(Rule child) {
		_children.add(child);
	}
	
	public List<Rule> getChildren() {
		return _children;
	}
	
}
