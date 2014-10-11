package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.util.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.json.*;
import java.util.HashMap;
import java.util.Map;
import com.mortardata.api.v2.Jobs;

public class gController extends DefaultDirectedGraph<gNode, gEdge> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8285085270334519490L;
	public Jobs mortarJobs_;
	public gController(EdgeFactory<gNode, gEdge> ef) {
		super(ef);
		// TODO Auto-generated constructor stub
	}

	public String exportToJSON(){
		
		Set<gNode> nodes = this.vertexSet();
		JSONObject my_obj_ = new JSONObject();
		
		JSONArray nodesJS = new JSONArray();
		for (gNode node : nodes )
		{
			JSONObject my_obj = new JSONObject();
			
			my_obj.put("id", node.getNodeId());
			my_obj.put("properties", node.exportToJSON());

			nodesJS.put(my_obj); 

			my_obj=null;
			
		}
		
		my_obj_.put("nodes", nodesJS);
		
		Set<gEdge> edges = this.edgeSet();
		
		JSONArray edgesJS = new JSONArray();
		for (gEdge edge : edges )
		{
			JSONObject my_obj = new JSONObject();
			
			my_obj.put("fromNode", this.getEdgeSource(edge).getNodeId() );
			my_obj.put("toNode", this.getEdgeTarget(edge).getNodeId());
			my_obj.put("properties", edge.exportToJSON());
			
			edgesJS.put(my_obj); 

			my_obj=null;
		}
		
		my_obj_.put("edges", edgesJS);
		
		return my_obj_.toString();
	}
	
	public Jobs getMortarJobs_() {
		return mortarJobs_;
	}

	public void setMortarJobs_(Jobs mortarJobs_) {
		this.mortarJobs_ = mortarJobs_;
	}

	public void importFromJSON(String json_string){
		Map<Long,gNode> map_ = new HashMap<Long,gNode>();
		
		JSONObject my_obj = new JSONObject(json_string);
		
		JSONArray nodes = new JSONArray(my_obj.get("nodes"));
		JSONArray edges = new JSONArray(my_obj.get("edges"));
		
		my_obj=null;
		
		for (int i = 0; i < nodes.length(); i++) {
			JSONObject my_node = new JSONObject(nodes.get(i));
			JSONObject my_op = (new JSONObject(my_node.getString("properties")));
			
			if (my_op.get("type")=="gMortarOperator")
			{
				gMortarOperator o = this.addMortarOperator(my_op);
				map_.put(my_node.getLong("id"), o);
			} else if (my_op.get("type")=="gCommandLineOperator")
			{
				gCommandLineOperator o = this.addCommandLineOperator(my_op);
				map_.put(my_node.getLong("id"), o);
			} else if (my_op.get("type")=="gII1Operator")
			{
				gII1Operator o = this.addII1Operator(my_op);
				map_.put(my_node.getLong("id"), o);	
			}
			my_node=null;
		}
		
		for (int i = 0; i < edges.length(); i++) {
			JSONObject my_edge = new JSONObject(edges.get(i));
			this.addEdge(map_.get(my_edge.getLong("fromNode")), map_.get(my_edge.getLong("toNode")));
			my_edge=null;
		}
		
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

	public gMortarOperator addMortarOperator(JSONObject obj){
		gMortarOperator node = new gMortarOperator();
		node.setJobs_(mortarJobs_);
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	public gMortarOperator addMortarOperator(String scriptPath){
		gMortarOperator node = new gMortarOperator(scriptPath);
		node.setJobs_(mortarJobs_);
		this.addVertex(node);
		return node;
	}
	
	public gCommandLineOperator addCommandLineOperator(String command){
		gCommandLineOperator node = new gCommandLineOperator(command);
		this.addVertex(node);
		return node;
	}
	public gCommandLineOperator addCommandLineOperator(JSONObject obj){
		gCommandLineOperator node = new gCommandLineOperator();
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	
	public gII1Operator addII1Operator(){
		gII1Operator node = new gII1Operator();
		this.addVertex(node);
		return node;
	}
	public gII1Operator addII1Operator(JSONObject obj){
		gII1Operator node = new gII1Operator();
		node.importFromJSON(obj);
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
