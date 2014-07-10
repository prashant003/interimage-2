package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.io.File;
import java.io.IOException;

import com.mortardata.api.v2.API;
import com.mortardata.api.v2.Jobs;
import com.mortardata.project.EmbeddedMortarProject;

public class graphExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		File pathToEmbeddedMortarProject = new File("/home/me/path/to/embedded/mortar/project");

		// github account associated with my Mortar account (to sync to backing github repo)
		String githubUsername = "myGithubUsername";
		String githubPassword = "myGithubPassword";
		String gitHash = "";
		// deploy embedded mortar project to Mortar (on branch master)
		EmbeddedMortarProject project = new EmbeddedMortarProject(pathToEmbeddedMortarProject);
		try {
			project.deployToMortar(githubUsername, githubPassword);
			gitHash = project.getGitMirrorURL();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String project_name = "mortar_example";
		int cluster_size = 2;

		//mortar user data
		String email = "darioaugusto@gmail.com";
		String apiKey = "EZdoTSiCVBnaCIYEDmtN6nBqrGYaiuH+mct5JAlf";
		
		Jobs jobs = new Jobs(new API(email, apiKey));
			
		gController g = new gController(null);
		
		gMortarOperator n1 = g.addMortarOperator("teste1");
		n1.setJobs_(jobs);
		n1.setClusterSize(cluster_size);
		n1.setProjectName(project_name);
		n1.setCodeVersion(gitHash);
		
		gMortarOperator n2 = g.addMortarOperator("teste2");
		n2.setJobs_(jobs);
		n2.setClusterSize(cluster_size);
		n2.setProjectName(project_name);
		n2.setCodeVersion(gitHash);
		
		gCommandLineOperator seg1 = g.addCommandLineOperator("interimage \"projectFile=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\segmentation\\segmentation.gap\" \"resourceImage=image#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.tif\" \"resourceShape=roi#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.shp\" \"outputDecisionRule=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.dt\" \"outputShapeFile=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\segmentation\\result.shp\" \"parameterOperator=@resolution@#1\" \"parameterOperator=@bands@#0,1,2,3\" \"parameterOperator=@weights@#1,1,1,1\" \"parameterOperator=@compactness@#0.5\" \"parameterOperator=@color@#0.5\" \"parameterOperator=@scale@#50\"");
		gCommandLineOperator lim1 = g.addCommandLineOperator("interimage \"projectFile=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\thresholding\\thresholding.gap\" \"resourceImage=image#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.tif\" \"resourceShape=roi#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.shp\" \"outputDecisionRule=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.dt\" \"outputShapeFile=C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\thresholding\\result.shp\" \"parameterOperator=@resolution@#1\" \"parameterOperator=@min@#0\" \"parameterOperator=@max@#0.33\" \"parameterOperator=@expression@#(R0:3 - R0:0) / (R0:3 + R0:0)\"");
		
		gII1Operator seg2 = g.addII1Operator();
		seg2.setII1Executable("interimage");
		seg2.setProjectFile("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\segmentation\\segmentation.gap");
		seg2.setResourceImage("image#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.tif");
		seg2.setResourceShape("roi#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.shp");
		seg2.setOutputDecisionRule("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.dt");
		seg2.setOutputShapeFile("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\segmentation\\result.shp");
		seg2.addParamater("resolution", "1");
		seg2.addParamater("bands", "0,1,2,3");
		seg2.addParamater("weights", "1,1,1,1");
		seg2.addParamater("compactness", "0.5");
		seg2.addParamater("color", "0.5");
		seg2.addParamater("scale", "50");
		
		gII1Operator lim2 = g.addII1Operator();
		lim2.setII1Executable("interimage");
		lim2.setProjectFile("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\thresholding\\thresholding.gap");
		lim2.setResourceImage("image#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.tif");
		lim2.setResourceShape("roi#C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.shp");
		lim2.setOutputDecisionRule("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\default.dt");
		lim2.setOutputShapeFile("C:\\Users\\Rodrigo\\Documents\\interimage-2\\interimage1_projects\\thresholding\\result.shp");
		lim2.addParamater("resolution", "1");
		lim2.addParamater("min", "0");
		lim2.addParamater("max", "0.33");
		lim2.addParamater("expression", "(R0:3 - R0:0) / (R0:3 + R0:0)");
		
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
		
		g.addEdge(n1, n2);
		g.addEdge(n2, seg1);
		g.addEdge(seg1, lim1);
		g.addEdge(lim1, seg2);
		g.addEdge(seg2, lim2);
		
		try {
		    Thread.sleep(1000);
		    g.run();
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
				
	}

}

// para fazer chamadas em java usando Pig mesmo
// http://wiki.apache.org/pig/EmbeddedPig
// http://stackoverflow.com/questions/11152068/run-pig-in-java-without-embedding-pig-script

// para fazer chamadas usando mortar em java
// https://github.com/mortardata/mortar-api-java

// para fazer chamadas usando mortar em python
// https://github.com/mortardata/mortar-api-python

// muito interessante para montar um projeto usando mortar
// http://help.mortardata.com/data_apps/build_your_own/developing_a_mortar_project
