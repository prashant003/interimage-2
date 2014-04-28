/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.geometry;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.geotools.dbffile.DbfFile;
import org.geotools.shapefile.ShapeHandler;
import org.geotools.shapefile.ShapeTypeNotSupportedException;
import org.geotools.shapefile.Shapefile;
import org.geotools.shapefile.ShapefileHeader;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jump.io.EndianDataInputStream;
import com.vividsolutions.jump.io.IllegalParametersException;

/**
 * Converts between Shapefile format and InterIMAGE internal format.<br>
 * @author Rodrigo Ferreira
 * TODO: Add tile info to the polygons
 */
public class ShapefileConverter {

	/**
	 * Converts from Shapefile to InterIMAGE internal format.<br>
	 * @param input shapefile path<br>
	 * output interimage file path
	 */	
	public static void shapefileToJson(String shapefile, String output, List<String> names, boolean keep) {
		
		try {
			
			/* Processing input parameters */
			if (shapefile == null) {
	            throw new IllegalParametersException("No shapefile specified");
	        }
			
	        int idx = shapefile.lastIndexOf(File.separatorChar);
	        String path = shapefile.substring(0, idx + 1); // ie. "/data1/hills.shp" -> "/data1/"
	        String fileName = shapefile.substring(idx + 1); // ie. "/data1/hills.shp" -> "hills.shp"

	        idx = fileName.lastIndexOf(".");

	        if (idx == -1) {
	            throw new IllegalParametersException("Filename must end in '.shp'");
	        }
	        
	        String fileNameWithoutExtention = fileName.substring(0, idx); // ie. "hills.shp" -> "hills"
	        String dbfFileName = path + fileNameWithoutExtention + ".dbf";
			
			/* Stream for output file */
			OutputStream out = new FileOutputStream(output);
			
			/* Preparing to read shapefile */
			InputStream in = new FileInputStream(shapefile);
			
			EndianDataInputStream file = new EndianDataInputStream(in);
	        
	        ShapefileHeader mainHeader = new ShapefileHeader(file);
	        	        
	        Geometry geom;
	        GeometryFactory factory = new GeometryFactory();

	        int type = mainHeader.getShapeType();
	        
	        ShapeHandler handler = Shapefile.getShapeHandler(type);
	        
	        if (handler == null) {
	        	in.close();
	        	out.close();
	        	throw new ShapeTypeNotSupportedException("Unsuported shape type: " + type);
	        }
	        
	        /* Preparing to read dbf file */
	        
	        File dbfFile = new File(dbfFileName);
	        DbfFile mydbf = null;

            if (dbfFile.exists()) {
            	mydbf = new DbfFile(dbfFileName);
            }
            
            int numfields = mydbf.getNumFields();
            
            /* Conversion */
	        int recordNumber=0;
	        int contentLength=0;
	        int tile = 1;

	        int index = 0;
	        
	        try {
	            while (true) {
	            	
	            	StringBuffer s = mydbf.GetDbfRec(index);
	            	
	            	List<String> attributeNames = new ArrayList<String>();
	            	
	            	recordNumber=file.readIntBE();
	                contentLength=file.readIntBE();                
	                geom = handler.read(file,factory,contentLength);
	
	                String str = "{\"geometry\":";	                
	                str += "\"" + geom.toText() + "\"";
	                str += ",\"data\":{\"0\":\"\"}";
	                str += ",\"properties\":{\"tile\":" + tile;
	                
	                for (int y=0; y<numfields; y++) {
	                	
	                	Object obj = mydbf.ParseRecordColumn(s, y);
	                		                	
	                	boolean bool;
	                	String name = mydbf.getFieldName(y);
	                	
	                	if (names.size() > 0) {	                	
		                	if (keep) {
		                		bool = names.contains(name);
		                	} else {
		                		bool = !names.contains(name);
		                	}
	                	} else {
	                		bool = true;
	                	}
	                	
	                	if (bool) {
		                	str += ",\"" + name + "\":";	                	
		                	attributeNames.add(name);
	                	
		                	try {
		                		//Tests if it's an integer
		                	    Integer intValue = Integer.parseInt(obj.toString().trim());
		                	    str += intValue.toString();
		                	} catch (NumberFormatException nfe) {
		                	    //Not an integer. Tests if it's a double
		                		try {
		                			Double doubleValue = Double.parseDouble(obj.toString().trim());
		                			str += doubleValue.toString();
		                		} catch (NumberFormatException nfe2) {
		                			//Not a double. Assuming it's a string
		                			str += "\"" + obj.toString().trim() + "\"";	
		                		}
		                	}
		                	
	                	}
	                		                	
	                }
	                
	                if (!attributeNames.contains("class")) {
	                	str += ",\"class\":";
	                	str += "\"None\"";
	                }
	                
	                if (!attributeNames.contains("id")) {
	                	str += ",\"id\":";
	                	str += recordNumber;
	                }
	                
	                str += "}}\n";
	                
	                out.write(str.getBytes());
	                
	            }
	        } catch (EOFException e) {
	        	
	        }
	        
            mydbf.close();
			
	        in.close();
	        out.close();
	        
		} catch (Exception e) {
			System.err.println("Failed to parse shapefile; error - " + e.getMessage());
		}
		
	}
	
	/**
	 * Converts from Shapefile to WKT.<br>
	 * @param input shapefile path<br>
	 * output WKT file path
	 */	
	@SuppressWarnings("unused")
	public static void shapefileToWKT(String shapefile, String output) {
		
		try {
			
			/* Processing input parameters */
			if (shapefile == null) {
	            throw new IllegalParametersException("No shapefile specified");
	        }
						
			/* Stream for output file */
			OutputStream out = new FileOutputStream(output);
			
			/* Preparing to read shapefile */
			InputStream in = new FileInputStream(shapefile);
			
			EndianDataInputStream file = new EndianDataInputStream(in);
	        
	        ShapefileHeader mainHeader = new ShapefileHeader(file);
	        	        
	        Geometry geom;
	        GeometryFactory factory = new GeometryFactory();

	        int type = mainHeader.getShapeType();
	        
	        ShapeHandler handler = Shapefile.getShapeHandler(type);
	        
	        if (handler == null) {
	        	in.close();
	        	out.close();
	        	throw new ShapeTypeNotSupportedException("Unsuported shape type: " + type);
	        }
	                    
            /* Conversion */
	        int recordNumber=0;
	        int contentLength=0;
	        
	        try {
	            while (true) {	            	
	            	recordNumber=file.readIntBE();
	                contentLength=file.readIntBE();                
	                geom = handler.read(file,factory,contentLength);
	                
	                String str = geom.toText() + "\n";
	                
	                out.write(str.getBytes());
	            }
	        } catch (EOFException e) {
	        	
	        }
			
	        in.close();
	        out.close();
	        
		} catch (Exception e) {
			System.err.println("Failed to parse shapefile; error - " + e.getMessage());
		}
		
	}
	
	/**
	 * Converts from InterIMAGE internal format to Shapefile.<br>
	 * @param interimage file path<br>
	 * output shapefile path
	 */	
	public static void JsonToShapefile(String input, String shapefile) {
		
	}
	
}
