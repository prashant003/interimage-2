package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.io.IOException;

public class gCommandLineOperator extends gNode {

	private String commandLine_;
	
	public gCommandLineOperator(String command){
		setCommandLine(command);
	}

	public String getCommandLine() {
		return commandLine_;
	}

	public void setCommandLine(String commandLine_) {
		this.commandLine_ = commandLine_;
	}

	@Override
	protected int execute() {
		// TODO Auto-generated method stub
		Runtime rt = Runtime.getRuntime();
		
		try {
			Process pr = rt.exec(commandLine_);
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
		
}

//1) Segmentação

//interimage "projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\segmentation.gap" "resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" "resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" "outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" "outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\result.shp" "parameterOperator=@resolution@#1" "parameterOperator=@bands@#0,1,2,3" "parameterOperator=@weights@#1,1,1,1" "parameterOperator=@compactness@#0.5" "parameterOperator=@color@#0.5" "parameterOperator=@scale@#50"

//2) Limiarização

//interimage "projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\thresholding.gap" "resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" "resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" "outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" "outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\result.shp" "parameterOperator=@resolution@#1" "parameterOperator=@min@#0" "parameterOperator=@max@#0.33" "parameterOperator=@expression@#(R0:3 - R0:0) / (R0:3 + R0:0)"