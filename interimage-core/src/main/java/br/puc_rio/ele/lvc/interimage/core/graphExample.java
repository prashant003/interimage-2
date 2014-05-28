package br.puc_rio.ele.lvc.interimage.core;

import java.io.File;
import java.io.IOException;

import com.mortardata.api.v2.API;
import com.mortardata.api.v2.Jobs;
import com.mortardata.project.EmbeddedMortarProject;

public class graphExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		File pathToEmbeddedMortarProject = new File("/path/to/embedded/project");

		// github account associated with my Mortar account (to sync to backing github repo)
		String githubUsername = "user";
		String githubPassword = "pass";
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

		String project_name = "interimage_2";
		int cluster_size = 2;

		//mortar user data
		String email = "usrname@email.com";
		String apiKey = "dsadjji+mct5JAlf";
		
		Jobs jobs = new Jobs(new API(email, apiKey));
  
		gController g = new gController(gEdge.class);
		
		gMortarNode n1 = g.addMortarOperator("s3n://interimage-tests/scripts/script1.pig");
		n1.setJobs_(jobs);
		n1.setClusterSize(cluster_size);
		n1.setProjectName(project_name);
		n1.setCodeVersion(gitHash);
		
		gMortarNode n2 = g.addMortarOperator("s3n://interimage-tests/scripts/script1.pig");
		n2.setJobs_(jobs);
		n2.setClusterSize(cluster_size);
		n2.setProjectName(project_name);
		n2.setCodeVersion(gitHash);
		
		g.addEdge(n1, n2);
		
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
