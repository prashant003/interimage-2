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
		
		gMortarNode n1 = g.addMortarOperator("teste1");
		n1.setJobs_(jobs);
		n1.setClusterSize(cluster_size);
		n1.setProjectName(project_name);
		n1.setCodeVersion(gitHash);
		
		gMortarNode n2 = g.addMortarOperator("teste2");
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
