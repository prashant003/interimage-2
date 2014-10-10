package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import org.jgrapht.graph.DefaultEdge;
import org.json.JSONObject;

public class gEdge extends DefaultEdge {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3311759278952307446L;
	private boolean activated_=true;
	private String rule_="";
	
	public boolean isActivated() {
		return activated_;
	}

	public void setActivated(boolean activated) {
		this.activated_ = activated;
	}
	
	public JSONObject exportToJSON() {
		JSONObject my_obj = new JSONObject();
		
		my_obj.put("rule", rule_);
		
		return my_obj;
	}
	
	public Boolean importFromJSON(JSONObject obj) {
		return true;
	}

	public String getRule_() {
		return rule_;
	}

	public void setRule_(String rule_) {
		this.rule_ = rule_;
	}
	
}
