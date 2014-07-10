package br.puc_rio.ele.lvc.interimage.core;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Locale;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import com.vividsolutions.jts.geom.Coordinate;

import br.puc_rio.ele.lvc.interimage.common.UTMLatLongConverter;
import br.puc_rio.ele.lvc.interimage.common.WebMercatorLatLongConverter;
import br.puc_rio.ele.lvc.interimage.core.project.Project;
import br.puc_rio.ele.lvc.interimage.geometry.ShapefileConverter;

public class Example {

	public static void main(String[] args) {

		Locale locale = new Locale("en", "US");
		Locale.setDefault(locale);
		
		/*int crsFromCode = 32722;
		
		Coordinate coord1 = new Coordinate(786505.273000, 7905730.859000);
		Coordinate coord2 = new Coordinate(787005.273000, 7905730.859000);
		Coordinate coord3 = new Coordinate(787005.273000, 7906230.859000);
		Coordinate coord4 = new Coordinate(786505.273000, 7906230.859000);
        
        //Coordinate coord2 = new Coordinate(bounds.getMaxX(), bounds.getMaxY());
		
		final int utmZone = (crsFromCode>32700) ? crsFromCode-32700 : crsFromCode-32600;
		final boolean southern = (crsFromCode>32700) ? true : false;
    
		final UTMLatLongConverter utm = new UTMLatLongConverter();
		utm.setDatum("WGS84");
						
		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
        webMercator.setDatum("WGS84");
		        
        utm.UTMToLatLong(coord1, utmZone, southern);
		
        System.out.println(coord1.toString());
        
        webMercator.LatLongToWebMercator(coord1);
        
        System.out.println(coord1.toString());
        
		utm.UTMToLatLong(coord2, utmZone, southern);
		
		System.out.println(coord2.toString());
		
		webMercator.LatLongToWebMercator(coord2);
		
		System.out.println(coord2.toString());
		
		utm.UTMToLatLong(coord3, utmZone, southern);
		
		System.out.println(coord3.toString());
		
		webMercator.LatLongToWebMercator(coord3);
		
		System.out.println(coord3.toString());
		
		utm.UTMToLatLong(coord4, utmZone, southern);
		
		System.out.println(coord4.toString());
		
		webMercator.LatLongToWebMercator(coord4);
				
		System.out.println(coord4.toString());*/
		
		//Project project = new Project();
		
		//C:\\Users\\Rodrigo\\Documents\\interimage\\interpretation_projects\\aquila\\aquila.gap
		//C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\exercise13.gap
		
		//project.readOldFile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\exercise13.gap");
			
		/*try {
		
		//TODO: treat other formats
		URL imageFile  = new URL("https://s3.amazonaws.com/interimage2/resources/images/image_T9268324953.tif");	        		
		
		URLConnection urlConn2 = imageFile.openConnection();
											
        urlConn2.connect();
        
        InputStream stream = new BufferedInputStream(urlConn2.getInputStream());
		
		
        ImageInputStream in = ImageIO.createImageInputStream(stream);
        
        if (in == null)
        	throw new Exception("Could not create input stream: " + imageFile.toString());
        
        
        Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
        //Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("tiff");
        
        if (!readers.hasNext())
        	throw new Exception("Could not find a reader: " + imageFile.toString());
        
        ImageReader reader = null;
        
        if (readers.hasNext()) {
            reader = readers.next();
            reader.setInput(in);           
        } else {
        	
        	//out.close();
	        throw new Exception("Unsuported image type");
        }
        
        ImageReadParam param = reader.getDefaultReadParam();

		BufferedImage buff = reader.read(0,param);
        
		System.out.println(buff.getWidth());
		
		} catch (Exception e) {
			
		}*/
				
		//RuleSet ruleSet = new RuleSet();
		//ruleSet.readOldFile("C:\\Users\\Rodrigo\\Desktop\\test.dt");
		
		//System.out.println(ruleSet.size());
		
		//System.out.println(ruleSet.getPigCode());
		
		//System.out.println(ruleSet.getCounts());
				
		//EPSG:32723
		
		ShapefileConverter.JSONToShapefile("C:\\Users\\Rodrigo\\Desktop\\part-m-00000","C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result6.shp", null, true, null, null);
		//ShapefileConverter.JSONToShapefile("C:\\Users\\Rodrigo\\Desktop\\part-r-00001","C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\result5.shp", null, true, null, null);
				
		//ShapefileConverter.WKTToShapefile("C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.wkt", "C:\\Users\\Rodrigo\\Documents\\workshop\\exercise13\\tiles.shp", null, null);
		
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
