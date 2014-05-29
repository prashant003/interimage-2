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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.geotools.dbffile.DbfFieldDef;
import org.geotools.dbffile.DbfFile;
import org.geotools.dbffile.DbfFileWriter;
import org.geotools.shapefile.ShapeHandler;
import org.geotools.shapefile.Shapefile;
import org.geotools.shapefile.ShapefileHeader;

import br.puc_rio.ele.lvc.interimage.common.UUID;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jump.io.EndianDataInputStream;
import com.vividsolutions.jump.io.EndianDataOutputStream;

/**
 * Converts between Shapefile format and InterIMAGE formats.<br>
 * @author Rodrigo Ferreira
 * TODO: Add tile info to the polygons
 */
public class ShapefileConverter {

	/**
	 * Converts from Shapefile to JSON format.<br>
	 * @param input shapefile path<br>
	 * output JSON file path
	 */	
	@SuppressWarnings("unused")
	public static void shapefileToJSON(String shapefile, String json, List<String> names, boolean keep) {
		
		try {
			
			/* Processing input parameters */
			if (shapefile == null) {
	            throw new Exception("No Shapefile specified");
	        } else {
	            if (shapefile.isEmpty()) {
	            	throw new Exception("No Shapefile specified");
	            }
	        }
			
			if (json == null) {
	            throw new Exception("No JSON specified");
	        } /*else {
	            if (json.isEmpty()) {
	            	throw new Exception("No JSON specified");
	            }
	        }*/
			
	        int idx = shapefile.lastIndexOf(File.separatorChar);
	        String path = shapefile.substring(0, idx + 1); // ie. "/data1/hills.shp" -> "/data1/"
	        String fileName = shapefile.substring(idx + 1); // ie. "/data1/hills.shp" -> "hills.shp"

	        idx = fileName.lastIndexOf(".");

	        if (idx == -1) {
	            throw new Exception("Filename must end in '.shp'");
	        }
	        
	        String fileNameWithoutExtention = fileName.substring(0, idx); // ie. "hills.shp" -> "hills"
	        String dbfFileName = path + fileNameWithoutExtention + ".dbf";
			
	        if (json.isEmpty()) {
	        	json = path + fileNameWithoutExtention + ".json";
	        }
	        
			/* Stream for output file */
			OutputStream out = new FileOutputStream(json);
			
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
	        	throw new Exception("Unsuported shape type: " + type);
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
	
	                //TODO: Should work with wkb
	                String str = "{\"geometry\":";	                
	                str += "\"" + geom.toText() + "\"";
	                str += ",\"data\":{\"0\":\"\"}";
	                str += ",\"properties\":{\"tile\":" + tile;
	                
	                for (int y=0; y<numfields; y++) {
	                	
	                	Object obj = mydbf.ParseRecordColumn(s, y);
	                		                	
	                	boolean bool;
	                	String name = mydbf.getFieldName(y);
	                	
	                	if (names != null) {
	                	
		                	if (names.size() > 0) {	                	
			                	if (keep) {
			                		bool = names.contains(name);
			                	} else {
			                		bool = !names.contains(name);
			                	}
		                	} else {
		                		bool = true;
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
	                
	                /*Computes object id as a hash of the geometry*/
	    		    
	    		    String id = new UUID(null).random();	        
	    		    	                
	                str += ",\"IIUUID\":";
	               	str += id;
	                	                
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
	
	private static void writeShapefileHeader(EndianDataOutputStream file, int fileLength, Envelope bounds) {
		
		try {
		
			int pos = 0;
	       // file.setLittleEndianMode(false);
	        file.writeIntBE(9994);
	        pos=pos+4;
	        for(int i=0;i<5;i++){
	            file.writeIntBE(0);//Skip unused part of header
	            pos+=4;
	        }
	        file.writeIntBE(fileLength);
	        pos+=4;
	        //file.setLittleEndianMode(true);
	        file.writeIntLE(1000);
	        pos+=4;
	        file.writeIntLE(2);
	        pos+=4;
	        //write the bounding box
	        file.writeDoubleLE(bounds.getMinX());
	        file.writeDoubleLE(bounds.getMinY());
	        file.writeDoubleLE(bounds.getMaxX());
	        file.writeDoubleLE(bounds.getMaxY());
	        pos+=8*4;
	        
	        //skip remaining unused bytes
	        //file.setLittleEndianMode(false);//well they may not be unused forever...
	        for(int i=0;i<4;i++){
	            file.writeDoubleLE(0.0);//Skip unused part of header
	            pos+=8;
	        }
	        
		} catch (Exception e) {
			System.err.println("Failed to write shapefile header; error - " + e.getMessage());
		}
		
	}
	
	private static void writeShapefileIndexHeader(EndianDataOutputStream file, int indexLength, Envelope bounds) {
		
		try {
		
			int pos = 0;
	        //file.setLittleEndianMode(false);
	        file.writeIntBE(9994);
	        pos=pos+4;
	        for(int i=0;i<5;i++){
	            file.writeIntBE(0);//Skip unused part of header
	            pos+=4;
	        }
	        file.writeIntBE(indexLength);
	        pos+=4;
	       // file.setLittleEndianMode(true);
	        file.writeIntLE(1000);
	        pos+=4;
	        file.writeIntLE(2);
	        pos+=4;
	        //write the bounding box
	        pos+=8;
	         file.writeDoubleLE(bounds.getMinX() );
	         pos+=8;
	         file.writeDoubleLE(bounds.getMinY() );
	         pos+=8;
	         file.writeDoubleLE(bounds.getMaxX() );
	         pos+=8;
	         file.writeDoubleLE(bounds.getMaxY() );
	         /*
	        for(int i = 0;i<4;i++){
	            pos+=8;
	            file.writeDouble(bounds[i]);
	        }*/
	        
	        //skip remaining unused bytes
	        //file.setLittleEndianMode(false);//well they may not be unused forever...
	        for(int i=0;i<4;i++){
	            file.writeDoubleLE(0.0);//Skip unused part of header
	            pos+=8;
	        }
	        
		} catch (Exception e) {
			System.err.println("Failed to write shapefile index header; error - " + e.getMessage());
		}
        		
	}
	
	/**
	 * Converts from JSON to Shapefile.<br>
	 * @param input JSON file path<br>
	 * output shapefile path
	 */	
	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public static void JSONToShapefile(String json, String shpFileName, List<String> names, boolean keep) {
	
		try {
			
			/*Processing input parameters*/
			if (json == null) {
	            throw new Exception("No JSON file specified");
	        } else {
	        	if (json.isEmpty()) {
	        		throw new Exception("No JSON file specified");
	        	}
	        }
	
			if (shpFileName == null) {
	            throw new Exception("No Shapefile specified");
	        } else {
	        	if (shpFileName.isEmpty()) {
	        		throw new Exception("No Shapefile specified");
	        	}
	        }			
			
			String path;
			String fileName;
			
	        int loc = shpFileName.lastIndexOf(File.separatorChar);
	        
	        if (loc == -1) {
	            // loc = 0; // no path - ie. "hills.shp"
	            // path = "";
	            // fname = shpfileName;
	            //probably using the wrong path separator character.
	            throw new Exception("Couldn't find the path separator character '" +
	                File.separatorChar +
	                "' in your shape file name. This you're probably using the unix (or dos) one.");
	        } else {
	            path = shpFileName.substring(0, loc + 1); // ie. "/data1/hills.shp" -> "/data1/"
	            fileName = shpFileName.substring(loc + 1); // ie. "/data1/hills.shp" -> "hills.shp"
	        }
	        
	        loc = fileName.lastIndexOf(".");
	
	        if (loc == -1) {
	            throw new Exception("Filename must end in '.shp'");
	        }
	
	        String fileNameWithoutExtention = fileName.substring(0, loc); // ie. "hills.shp" -> "hills."
	        String dbfFileName = path + fileNameWithoutExtention + ".dbf";
	        
	        int numRecords = 0;
	        Envelope bounds = null;
	        int fileLength = 0;	        
	        
	        double[] boundsArr = new double[4];
	        
	        /*Read all the records in the json file and compute some info for the headers*/
	        
	        InputStream in1 = new FileInputStream(json);
	        InputStreamReader inStream1 = new InputStreamReader(in1);
	        BufferedReader buff1 = new BufferedReader(inStream1);
	        
	        JsonFactory jfactory = new JsonFactory();
	        
	        GeometryParser geometryParser = new GeometryParser();
	        
	        ShapeHandler handler = null;
	        
	        List<DbfFieldDef> fieldDefs = new ArrayList<DbfFieldDef>();
	        
	        String line1;
	        while ((line1 = buff1.readLine()) != null) {
	        	
	        	Geometry geometry;	        	
	        	JsonParser jParser = jfactory.createJsonParser(line1);
	        	
	        	int countEndObject = 0;
	        	
	        	while (true) {
	        		
	        		JsonToken token = jParser.nextToken();
	        		
	        		if (token==JsonToken.END_OBJECT)
	        			countEndObject++;
	        		
	        		if (countEndObject>1) //skip data '}' character
	        			break;
	        		
	        		String fieldname = jParser.getCurrentName();
	        		
	        		if ("geometry".equals(fieldname)) {
	        			jParser.nextToken();
	        			
	        			//TODO: Should work with wkb
	        			geometry = geometryParser.parseGeometry(jParser.getText());
	        			
	        			Envelope envelope = geometry.getEnvelopeInternal();
	        			
	        			if (envelope.getMinX() < boundsArr[0])
	        				boundsArr[0] = envelope.getMinX();
	        			
	        			if (envelope.getMinY() < boundsArr[1])
	        				boundsArr[1] = envelope.getMinY();
	        			
	        			if (envelope.getMaxX() > boundsArr[2])
	        				boundsArr[2] = envelope.getMaxX();
	        			
	        			if (envelope.getMaxY() > boundsArr[3])
	        				boundsArr[3] = envelope.getMaxY();
	        			
	        			if (handler == null) {
	    	            	handler = Shapefile.getShapeHandler(geometry,2);
	    	            }
	        			
	        			fileLength=fileLength + handler.getLength(geometry);
	     	            fileLength+=4;//for each header
	     	            
	        		}
	        		
	        		/*Just read the first record to get the fields info for the dbf file*/
	        		if ((numRecords==0) && ("properties".equals(fieldname))) {
	        			
	        			jParser.nextToken(); //skip '{' character
	        			
	        			while (jParser.nextToken() != JsonToken.END_OBJECT) {

	        				String columnName = jParser.getText();
	        				
	        				boolean bool;
		                	
		                	if (names != null) {
		                	
			                	if (names.size() > 0) {	                	
				                	if (keep) {
				                		bool = names.contains(columnName);
				                	} else {
				                		bool = !names.contains(columnName);
				                	}
			                	} else {
			                		bool = true;
			                	}
			                	
		                	} else {
		                		bool = true;
		                	}
	        				
	        				jParser.nextToken();
	        					        				
	        				if (bool) {
	        					
	        					String value = jParser.getText().trim();
	        				
		        				try {
			                		//Tests if it's an integer
			                	    Integer intValue = Integer.parseInt(value);
			                	    fieldDefs.add(new DbfFieldDef(columnName, 'N', 16, 0));
			                	} catch (NumberFormatException nfe) {
			                	    //Not an integer. Tests if it's a double
			                		try {
			                			Double doubleValue = Double.parseDouble(value);
			                			fieldDefs.add(new DbfFieldDef(columnName, 'N', 33, 16));
			                		} catch (NumberFormatException nfe2) {
			                			//Not a double. Assuming it's a string
			                			fieldDefs.add(new DbfFieldDef(columnName, 'C', 255, 0));		                				
			                		}
			                	}
		        				
	        				}
	        				
	        				
	        			}
	        			
	        		}
	        		
	        	}
	        	
	        	numRecords++;
	        	
	        	jParser.close();
	        }
	        
	        buff1.close();
	        
	        bounds = new Envelope(boundsArr[0], boundsArr[2], boundsArr[1], boundsArr[3]);
	        
	        /*Preparing to write dbf file*/
	        DbfFileWriter dbf;
	        dbf = new DbfFileWriter(dbfFileName);
	        
	        DbfFieldDef[] fields = new DbfFieldDef[fieldDefs.size()];
	        
	        int countf = 0;
	        for (DbfFieldDef f : fieldDefs) {
	        	fields[countf] = f;
	        	countf++;
	        }
	        
	        /*Writing dbf file header*/
	        dbf.writeHeader(fields, numRecords);
	        
	        /*Preparing to write shapefile*/
	        OutputStream out2 = new FileOutputStream(shpFileName);
			EndianDataOutputStream shapeFile = new EndianDataOutputStream(out2);
						
			/*Writing shapefile header*/
			writeShapefileHeader(shapeFile, fileLength, bounds);
	        
	        /*Preparing to write index file*/
	        String shxFileName = path + fileNameWithoutExtention + ".shx";
	        BufferedOutputStream out3 = new BufferedOutputStream(new FileOutputStream(shxFileName));
	        EndianDataOutputStream indexfile = new EndianDataOutputStream(out3);

	        /*Writing index file header*/
	        int indexLength = 0;
	        indexLength = 50+(4*numRecords);
	        writeShapefileIndexHeader(indexfile, indexLength, bounds);
	        
	        /*Reads json file again, but now writing the shapefile, index file and dbf file*/
	        InputStream in = new FileInputStream(json);
	        InputStreamReader inStream = new InputStreamReader(in);
	        BufferedReader buff = new BufferedReader(inStream);
	        	        
	        int indexPos = 50;
	        int indexLen = 0;
	        
	        int pos = 50;
	        
	        int count = 1;
	        
	        String line;
	        while ((line = buff.readLine()) != null) {
	        	
	        	Vector DBFrow = new Vector();
	        	
	        	Geometry geometry;	        	
	        	JsonParser jParser = jfactory.createJsonParser(line);
	        	
	        	int countEndObject = 0;
	        	
	        	while (true) {
	        		
	        		JsonToken token = jParser.nextToken();
	        		
	        		if (token==JsonToken.END_OBJECT)
	        			countEndObject++;
	        		
	        		if (countEndObject>1) //skip data '}' character
	        			break;
	        		
	        		String fieldname = jParser.getCurrentName();
	        		
	        		if ("geometry".equals(fieldname)) {
	        			jParser.nextToken();
	        			
	        			//TODO: Should work with wkb
	        			geometry = geometryParser.parseGeometry(jParser.getText());
	        			
	        			if (handler == null) {
	    	            	handler = Shapefile.getShapeHandler(geometry,2);
	    	            }
	        			
	        			/*Writing to index file*/
	        			indexLen = handler.getLength(geometry);	        	        
	        			indexfile.writeIntBE(indexPos);
	        			indexfile.writeIntBE(indexLen);
	                    indexPos = indexPos+indexLen+4;
	        			
	                    /*Writing to shapefile*/
	                    shapeFile.writeIntBE(count++);
	                    shapeFile.writeIntBE(handler.getLength(geometry));
	                    // file.setLittleEndianMode(true);
	                    pos=pos+4; // length of header in WORDS
	                    handler.write(geometry,shapeFile);
	                    pos+=handler.getLength(geometry); // length of shape in WORDS
	                    
	        		}
	        		
	        		if ("properties".equals(fieldname)) {
	        			
	        			jParser.nextToken(); //skip '{' character
	        			
	        			while (jParser.nextToken() != JsonToken.END_OBJECT) {

	        				String columnName = jParser.getText();
	        				
	        				boolean bool;
		                	
		                	if (names != null) {
		                	
			                	if (names.size() > 0) {	                	
				                	if (keep) {
				                		bool = names.contains(columnName);
				                	} else {
				                		bool = !names.contains(columnName);
				                	}
			                	} else {
			                		bool = true;
			                	}
			                	
		                	} else {
		                		bool = true;
		                	}
	        				
	        				jParser.nextToken();
	        				
	        				if (bool) {
	        				
		        				String value = jParser.getText().trim();
		        				
		        				try {
			                		//Tests if it's an integer
			                	    Integer intValue = Integer.parseInt(value);
			                	    DBFrow.add(intValue);
			                	} catch (NumberFormatException nfe) {
			                	    //Not an integer. Tests if it's a double
			                		try {
			                			Double doubleValue = Double.parseDouble(value);
			                			DBFrow.add(doubleValue);
			                		} catch (NumberFormatException nfe2) {
			                			//Not a double. Assuming it's a string
			                			DBFrow.add(value);	
			                		}
			                	}
		        				
	        				}
	        				
	        			}
	        			
	        		}
	        			        		
	        	}
	        	
	        	/*Writing to dbf file*/
	        	dbf.writeRecord(DBFrow);
	        	jParser.close();
	        }
	        
	        shapeFile.close();
	        indexfile.close();
	        dbf.close();
	        buff.close();
	        	        
		} catch (Exception e) {
			System.err.println("Failed to create shapefile; error - " + e.getMessage());
		}
		
	}
	
	/**
	 * Converts from Shapefile to WKT.<br>
	 * @param input shapefile path<br>
	 * output WKT file path
	 */	
	@SuppressWarnings("unused")
	public static void shapefileToWKT(String shapefile, String wkt) {
		
		try {
			
			/* Processing input parameters */
			if (shapefile == null) {
	            throw new Exception("No Shapefile specified");
	        } else {
	        	if (shapefile.isEmpty()) {
	        		throw new Exception("No Shapefile specified");
	        	}
	        }
			
			if (wkt == null) {
	            throw new Exception("No WKT file specified");
	        } else {
	        	if (wkt.isEmpty()) {
	        		throw new Exception("No WKT file specified");
	        	}
	        }
			
			/* Stream for output file */
			OutputStream out = new FileOutputStream(wkt);
			
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
	        	throw new Exception("Unsuported shape type: " + type);
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
	 * Converts from WKT to Shapefile.<br>
	 * @param input WKT file path<br>
	 * output shapefile path
	 */	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void WKTToShapefile(String wkt, String shpFileName) {
		
		try {
		
			/*Processing input parameters*/
			if (wkt == null) {
	            throw new Exception("No WKT file specified");
	        } else {
	        	if (wkt.isEmpty()) {
	        		throw new Exception("No WKT file specified");
	        	}
	        }
	
			if (shpFileName == null) {
	            throw new Exception("No Shapefile specified");
	        } else {
	        	if (shpFileName.isEmpty()) {
	        		throw new Exception("No Shapefile specified");
	        	}
	        }
			
			String path;
			String fileName;
			
	        int loc = shpFileName.lastIndexOf(File.separatorChar);
	        
	        if (loc == -1) {
	            // loc = 0; // no path - ie. "hills.shp"
	            // path = "";
	            // fname = shpfileName;
	            //probably using the wrong path separator character.
	            throw new Exception("Couldn't find the path separator character '" +
	                File.separatorChar +
	                "' in your shape file name. This you're probably using the unix (or dos) one.");
	        } else {
	            path = shpFileName.substring(0, loc + 1); // ie. "/data1/hills.shp" -> "/data1/"
	            fileName = shpFileName.substring(loc + 1); // ie. "/data1/hills.shp" -> "hills.shp"
	        }
	        
	        loc = fileName.lastIndexOf(".");
	
	        if (loc == -1) {
	            throw new Exception("Filename must end in '.shp'");
	        }
	
	        String fileNameWithoutExtention = fileName.substring(0, loc); // ie. "hills.shp" -> "hills."
	        String dbfFileName = path + fileNameWithoutExtention + ".dbf";
	        
	        int numRecords = 0;
	        Envelope bounds = null;
	        int fileLength = 0;	        
	        
	        double[] boundsArr = new double[4];
	        
	        /*Read all the geometries in WKT file and compute some info for the headers*/
	        
	        InputStream in1 = new FileInputStream(wkt);
	        InputStreamReader inStream1 = new InputStreamReader(in1);
	        BufferedReader buff1 = new BufferedReader(inStream1);
	        
	        GeometryParser geometryParser = new GeometryParser();
	        
	        ShapeHandler handler = null;
	        
	        List<DbfFieldDef> fieldDefs = new ArrayList<DbfFieldDef>();
	        
			String line1;
	        while ((line1 = buff1.readLine()) != null) {
	        	
	        	Geometry geometry = geometryParser.parseGeometry(line1);
    			
    			Envelope envelope = geometry.getEnvelopeInternal();
    			
    			if (envelope.getMinX() < boundsArr[0])
    				boundsArr[0] = envelope.getMinX();
    			
    			if (envelope.getMinY() < boundsArr[1])
    				boundsArr[1] = envelope.getMinY();
    			
    			if (envelope.getMaxX() > boundsArr[2])
    				boundsArr[2] = envelope.getMaxX();
    			
    			if (envelope.getMaxY() > boundsArr[3])
    				boundsArr[3] = envelope.getMaxY();
    			
    			if (handler == null) {
	            	handler = Shapefile.getShapeHandler(geometry,2);
	            }
    			
    			fileLength=fileLength + handler.getLength(geometry);
 	            fileLength+=4;//for each header
	            
 	            numRecords++;
 	            
	        }
	        	        
	        buff1.close();
	        
	        /*Preparing simple DBF header*/ 	            
	        fieldDefs.add(new DbfFieldDef("id", 'N', 16, 0));
	        
	        bounds = new Envelope(boundsArr[0], boundsArr[2], boundsArr[1], boundsArr[3]);
	        
	        /*Preparing to write dbf file*/
	        DbfFileWriter dbf;
	        dbf = new DbfFileWriter(dbfFileName);
	        
	        DbfFieldDef[] fields = new DbfFieldDef[fieldDefs.size()];
	        
	        int countf = 0;
	        for (DbfFieldDef f : fieldDefs) {
	        	fields[countf] = f;
	        	countf++;
	        }
	        
	        /*Writing dbf file header*/
	        dbf.writeHeader(fields, numRecords);
        
	        /*Preparing to write shapefile*/
	        OutputStream out2 = new FileOutputStream(shpFileName);
			EndianDataOutputStream shapeFile = new EndianDataOutputStream(out2);
						
			/*Writing shapefile header*/
			writeShapefileHeader(shapeFile, fileLength, bounds);
	        
	        /*Preparing to write index file*/
	        String shxFileName = path + fileNameWithoutExtention + ".shx";
	        BufferedOutputStream out3 = new BufferedOutputStream(new FileOutputStream(shxFileName));
	        EndianDataOutputStream indexfile = new EndianDataOutputStream(out3);

	        /*Writing index file header*/
	        int indexLength = 0;
	        indexLength = 50+(4*numRecords);
	        writeShapefileIndexHeader(indexfile, indexLength, bounds);
	        
	        /*Reads WKT file again, but now writing the shapefile, index file and dbf file*/
	        InputStream in = new FileInputStream(wkt);
	        InputStreamReader inStream = new InputStreamReader(in);
	        BufferedReader buff = new BufferedReader(inStream);
	        	        
	        int indexPos = 50;
	        int indexLen = 0;
	        
	        int pos = 50;
	        
	        int count = 1;
	        
	        String line;
	        while ((line = buff.readLine()) != null) {
	        	
	        	Vector DBFrow = new Vector();
	        	
	        	Geometry geometry = geometryParser.parseGeometry(line);
    			
    			if (handler == null) {
	            	handler = Shapefile.getShapeHandler(geometry,2);
	            }
    			
    			/*Writing to index file*/
    			indexLen = handler.getLength(geometry);	        	        
    			indexfile.writeIntBE(indexPos);
    			indexfile.writeIntBE(indexLen);
                indexPos = indexPos+indexLen+4;
    			
                /*Writing to shapefile*/
                shapeFile.writeIntBE(count);
                shapeFile.writeIntBE(handler.getLength(geometry));
                // file.setLittleEndianMode(true);
                pos=pos+4; // length of header in WORDS
                handler.write(geometry,shapeFile);
                pos+=handler.getLength(geometry); // length of shape in WORDS
	        	
                Integer intValue = count++;
        	    DBFrow.add(intValue);
                
        	    dbf.writeRecord(DBFrow);
        	            	    
	        }
	        
	        shapeFile.close();
	        indexfile.close();
	        dbf.close();
	        buff.close();
	        
		} catch (Exception e) {
			System.err.println("Failed to create shapefile; error - " + e.getMessage());
		}
	        
	}
	
}
