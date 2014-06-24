package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class gII1Operator extends gNode {

	private String II1Executable_;
	private String projectFile_;
	private String resourceImage_;
	private String resourceShape_;
	private String outputDecisionRule_;
	private String outputShapeFile_;
	private Map<String,String> parameterList_ = new HashMap<String,String>();
	
	@Override
	protected int execute() {
		// TODO Auto-generated method stub
		String command = buildCommand();
		
		Runtime rt = Runtime.getRuntime();
		
		try {
			Process pr = rt.exec(command);
			int value = pr.waitFor();
			return value;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}
	
	private String buildCommand() {

		String command = II1Executable_+ " " + "projectFile=" + projectFile_ + " " ;
		command = command + "resourceImage=" + resourceImage_ + " ";
		command = command + "resourceShape=" + resourceShape_ + " ";
		command = command + "outputDecisionRule=" + outputDecisionRule_ + " ";
		command = command + "outputShapeFile=" + outputShapeFile_ + " ";
		
		for (Map.Entry<String, String> entry : this.parameterList_.entrySet()) {
			command = command + "parameterOperator=@" + entry.getKey() + "@#" + entry.getValue() + " ";			
		}
				
		return command;
	}

	public String getProjectFile() {
		return projectFile_;
	}

	public void setProjectFile(String projectFile) {
		this.projectFile_ = projectFile;
	}

	public String getResourceImage() {
		return resourceImage_;
	}

	public void setResourceImage(String resourceImage) {
		this.resourceImage_ = resourceImage;
	}

	public String getResourceShape() {
		return resourceShape_;
	}

	public void setResourceShape(String resourceShape) {
		this.resourceShape_ = resourceShape;
	}

	public String getOutputDecisionRule() {
		return outputDecisionRule_;
	}

	public void setOutputDecisionRule(String outputDecisionRule) {
		this.outputDecisionRule_ = outputDecisionRule;
	}

	public String getOutputShapeFile() {
		return outputShapeFile_;
	}

	public void setOutputShapeFile(String outputShapeFile) {
		this.outputShapeFile_ = outputShapeFile;
	}

	public void addParamater(String name, String value) {
		this.parameterList_.put(name, value);
	}
	
	public void changeParamater(String name, String value) {
		this.parameterList_.put(name, value);
	}

	public String getII1Executable() {
		return II1Executable_;
	}

	public void setII1Executable(String II1Executable) {
		this.II1Executable_ = II1Executable;
	}
	
}

//1) Segmentação

//interimage 
//"projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\segmentation.gap" 
//"resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" 
//"resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" 
//"outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" 
//"outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\result.shp" 
//"parameterOperator=@resolution@#1" 
//"parameterOperator=@bands@#0,1,2,3" 
//"parameterOperator=@weights@#1,1,1,1" 
//"parameterOperator=@compactness@#0.5" 
//"parameterOperator=@color@#0.5" 
//"parameterOperator=@scale@#50"

//2) Limiarização

//interimage 
//"projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\thresholding.gap" 
//"resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" 
//"resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" 
//"outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" 
//"outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\result.shp" 
//"parameterOperator=@resolution@#1" 
//"parameterOperator=@min@#0" 
//"parameterOperator=@max@#0.33" 
//"parameterOperator=@expression@#(R0:3 - R0:0) / (R0:3 + R0:0)"