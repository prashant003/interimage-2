package br.puc_rio.ele.lvc.interimage.core;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import com.vividsolutions.jts.geom.Coordinate;

import br.puc_rio.ele.lvc.interimage.common.UTMLatLongConverter;
import br.puc_rio.ele.lvc.interimage.common.WebMercatorLatLongConverter;
import br.puc_rio.ele.lvc.interimage.core.project.Project;
import br.puc_rio.ele.lvc.interimage.core.ruleset.RuleSet;
import br.puc_rio.ele.lvc.interimage.core.operatorgraph.OperatorSet;
import br.puc_rio.ele.lvc.interimage.core.operatorgraph.PigParser;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;

public class Example {

	public static void main(String[] args) {

		Locale locale = new Locale("en", "US");
		Locale.setDefault(locale);
				
		PigParser parser = new PigParser();
		
		Properties properties = new Properties();
		
		properties.setProperty("interimage.projectPath", "C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\");
		properties.setProperty("interimage.sourceURL", "https://s3.amazonaws.com/interimage2/");
		properties.setProperty("interimage.sourceSpecificURL", "s3n://interimage2/");
		properties.setProperty("interimage.projectName", "exercise13");
		properties.setProperty("interimage.parallel", "20");
		properties.setProperty("interimage.crs", "EPSG:32735");
		properties.setProperty("interimage.tileSizeMeters", "640");
						
		Map<String, String> params = new HashMap<String,String>();
		
		/*params.put("$IMAGE_KEY","nairobi");
		params.put("$THRESHOLDS","0.33,1");
		params.put("$CLASSES","Vegetation");
		params.put("$OPERATION","Index(0,1)");
		params.put("$RELIABILITY","0.3");
		params.put("$OUTPUT.ROI","true");
		//params.put("$STORE","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		System.out.println(parser.parse("Limiarization"));
		
		System.out.println("EXEC;\n");
		
		params = new HashMap<String,String>();*/
		
		/*params.put("$IMAGE_KEY","nairobi");
		params.put("$SCALE","30");
		params.put("$WCOLOR","0.5");
		params.put("$WCOMPACTNESS","0.5");
		params.put("$WBANDS","1,1,1,1");
		//params.put("$ROI",parser.getExport().get("Soil"));
		params.put("$CLASS","Segmentation");
		params.put("$RELIABILITY","0.3");
		//params.put("$INPUT.ROI","true");
		//params.put("$STORE","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		try{
		FileOutputStream file = new FileOutputStream("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\script.pig");
		file.write(parser.parse("MutualMultiresolutionSegmentation").getBytes());
		file.close();
		} catch(Exception e) {
			
		}*/
		
		//System.out.println(parser.getExport());
								
		/*RuleSet ruleSet = new RuleSet();
		//ruleSet.setup(properties);
		//ruleSet.setCounts(parser.getGlobalRelationMap());
		//ruleSet.setLastRelation(parser.getGlobalRelations().get(parser.getGlobalRelations().size()-1));
		ruleSet.readOldFile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\NonUrban.dt");
		
		//System.out.println(ruleSet.size());
				
		System.out.println("EXEC;\n");
		
		params = new HashMap<String,String>();
		
		params.put("$CLASS","NonUrban");
		//params.put("$CLASSES","NonUrban");
		//params.put("$STORE","true");
		params.put("$RELIABILITY","0.3");
		params.put("$OUTPUT.ROI","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		System.out.println(parser.parse(ruleSet.getPigCode()));
		
		//System.out.println(parser.getExport());
		
		System.out.println("EXEC;\n");
		
		params = new HashMap<String,String>();
		
		params.put("$IMAGE_KEY","libreville");
		params.put("$SCALE","20");
		params.put("$WCOLOR","0.3");
		params.put("$WCOMPACTNESS","0.1");
		params.put("$WBANDS","1,1,1,1");
		params.put("$ROI",parser.getExport().get("NonUrban"));
		params.put("$CLASS","Rural");
		params.put("$RELIABILITY","0.3");
		params.put("$INPUT.ROI","true");
		//params.put("$STORE","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		System.out.println(parser.parse("MutualMultiresolutionSegmentation"));
				
		ruleSet = new RuleSet();
		//ruleSet.setup(properties);
		//ruleSet.setCounts(parser.getGlobalRelationMap());
		//ruleSet.setLastRelation(parser.getGlobalRelations().get(parser.getGlobalRelations().size()-1));
		ruleSet.readOldFile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\Rural.dt");
		
		//System.out.println(ruleSet.size());
				
		System.out.println("EXEC;\n");
		
		params = new HashMap<String,String>();
		
		params.put("$CLASS","Rural");
		//params.put("$CLASSES","NonUrban");
		//params.put("$STORE","true");
		params.put("$RELIABILITY","0.3");
		params.put("$OUTPUT.ROI","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		System.out.println(parser.parse(ruleSet.getPigCode()));
		
		System.out.println("EXEC;\n");
		
		params = new HashMap<String,String>();
		
		params.put("$IMAGE_KEY","libreville");
		params.put("$THRESHOLDS","-1,0.28,1");
		params.put("$CLASSES","Vegetation,Mangrove");
		params.put("$OPERATION","Index(2,0)");
		params.put("$INPUT.ROI","true");
		params.put("$ROI",parser.getExport().get("Rural"));
		params.put("$RELIABILITY","0.3");
		params.put("$OUTPUT.ROI","true");
		//params.put("$STORE","true");
		
		parser.setup(properties);
		parser.setParams(params);
		
		System.out.println(parser.parse("Limiarization"));*/
		
		//Project project = new Project();
			
		//project.readOldFile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\exercise13.gap");
		
		//EPSG:32723
		
		//ShapefileConverter.JSONToShapefile("C:\\Users\\Rodrigo\\Desktop\\part-m-000042","C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result1.shp", null, true, null, null);
		//ShapefileConverter.JSONToShapefile("C:\\Users\\Rodrigo\\Desktop\\part-r-00001","C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result2.shp", null, true, null, null);
				
		//ShapefileConverter.WKTToShapefile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.wkt", "C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.shp", null, null);
		//ShapefileConverter.WKTToShapefile("C:\\Users\\Rodrigo\\Desktop\\results1\\Vegetation.wkt", "C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\Vegetation.shp", null, null);		
		//ShapefileConverter.WKTToShapefile("C:\\Users\\Rodrigo\\Documents\\Google\\training_data\\training.wkt", "C:\\Users\\Rodrigo\\Documents\\Google\\training_data\\training.shp", null, null);
		
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
