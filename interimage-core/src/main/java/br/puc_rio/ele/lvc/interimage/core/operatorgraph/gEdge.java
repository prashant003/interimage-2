package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import org.jgrapht.graph.DefaultEdge;

public class gEdge extends DefaultEdge {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3311759278952307446L;
	private boolean activated=true;

	public boolean isActivated() {
		return activated;
	}

	public void setActivated(boolean activated) {
		this.activated = activated;
	}
	
}
