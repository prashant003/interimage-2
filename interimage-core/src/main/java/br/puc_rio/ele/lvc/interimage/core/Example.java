package br.puc_rio.ele.lvc.interimage.core;

import java.util.Locale;

import br.puc_rio.ele.lvc.interimage.core.project.Project;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;

public class Example {

	public static void main(String[] args) {

		Locale locale = new Locale("en", "US");
		Locale.setDefault(locale);
		
		Project project = new Project();
		
		project.readOldFile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\exercise13.gap");
		
		//EPSG:32723
		
		ShapefileConverter.JSONToShapefile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result.json","C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result2.shp", null, true, "EPSG:3857");
		
		ShapefileConverter.WKTToShapefile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.wkt", "C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.shp", "EPSG:3857");
		
		//System.out.println(project.getProject());
		//System.out.println(project.getImageList().size());
		//System.out.println(project.getShapeList().size());
		
		//SemanticNetwork semNet = project.getSemanticNetwork();
				
		//Source source = new AWSSource("","","interimage2");
		
		//source.put("C:\\Users\\Rodrigo\\Documents\\interimage-2\\workspace\\interimage-geometry-helper\\test2.wkt", "resources/shapes/teste.wkt");
		
		//System.out.println(semNet.size());
		
		/*System.out.println(project.getImageList().getGeoWest());
		System.out.println(project.getImageList().getGeoNorth());
		System.out.println(project.getImageList().getGeoEast());
		System.out.println(project.getImageList().getGeoSouth());
		
		System.out.println(project.getMinResolution());
		
		System.out.println(project.getGeoTileSize());*/
		
	}

}
