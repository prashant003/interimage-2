package br.puc_rio.ele.lvc.interimage.core;

import java.util.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;

public class gController extends DefaultDirectedGraph<gNode, gEdge> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8285085270334519490L;

	public gController(EdgeFactory<gNode, gEdge> ef) {
		super(ef);
		// TODO Auto-generated constructor stub
	}

	//update inputs from successor nodes with output from node
	public void updateLinkedNodes(gNode node){
		Set<gEdge> relatedEdges = this.outgoingEdgesOf(node);
		for (gEdge outgoing : relatedEdges )
		{
			if (outgoing.isActivated())
				this.getEdgeTarget(outgoing).improveRequest();
		}
		
	}
	
	//evaluate the input data and the requests for executing 
	public void evaluateNodeInputs(gNode node){
		if (node.getRequests()==this.inDegreeOf(node))
			if (!node.isExecuted())
				node.setAvailable(true);
	}
	
	public gMortarNode addMortarOperator(String scriptPath){
		gMortarNode node = new gMortarNode(scriptPath);
		this.addVertex(node);
		return node;
	}
	
	//run controller
	public int run()
	{
		int numberOfRunningNodes=0;
		Set<gNode> nodes = this.vertexSet();
		
		for (gNode node : nodes )
		{
			if (node.isRunning())
			{
				numberOfRunningNodes=numberOfRunningNodes+1;
				continue;
			}

			if (node.isExecuted())
			{
				updateLinkedNodes(node);	
				continue;
			}
			
			evaluateNodeInputs(node);
			
			if (node.isAvailable())
			{
				//call node run method
				node.run();
			}

		}
		return numberOfRunningNodes;
	}
	
}
