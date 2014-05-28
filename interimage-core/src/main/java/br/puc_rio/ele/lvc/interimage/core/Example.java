package br.puc_rio.ele.lvc.interimage.core;

import br.puc_rio.ele.lvc.interimage.core.project.Project;
import br.puc_rio.ele.lvc.interimage.core.semanticnetwork.SemanticNetwork;

public class Example {

	public static void main(String[] args) {

		Project project = new Project();
		
		project.readOldFile("C:\\Users\\Rodrigo\\Documents\\interimage\\interpretation_projects\\ii_batch\\ii_batch.gap");
		
		//System.out.println(project.getProject());
		//System.out.println(project.getImageList().size());
		//System.out.println(project.getShapeList().size());
		
		SemanticNetwork semNet = project.getSemanticNetwork();
				
		//System.out.println(semNet.size());
		
		/*System.out.println(project.getImageList().getGeoWest());
		System.out.println(project.getImageList().getGeoNorth());
		System.out.println(project.getImageList().getGeoEast());
		System.out.println(project.getImageList().getGeoSouth());
		
		System.out.println(project.getMinResolution());
		
		System.out.println(project.getGeoTileSize());*/
		
	}

}
