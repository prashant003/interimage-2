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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
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
	 * Converts from Shapefile to JSON format.<br><br>
	 * This method also:<br>
	 * 1) Converts the reference system<br>
	 * 2) Computes the tiles for the polygons<br>
	 * @param shapefile - input shapefile path<br>
	 * json - output JSON file path<br>
	 * names - a comma-separated list of attribute names<br>
	 * keep - tells if the method should keep or remove the listed attributes<br>
	 * epsgFrom - input EPSG code in the form "EPSG:0000"<br>
	 * geoBBox - the method will store in this vector the bbox of the shapefile<br>
	 * tileManager - TileManager object
	 */	

	public static void shapefileToJSON(String shapefile, String json, List<String> names, boolean keep, String epsgFrom, double[] geoBBox, TileManager tileManager) {
		
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
	        } else {
	            if (json.isEmpty()) {
	            	throw new Exception("No JSON specified");
	            }
	        }
						
			int epsgFromCode = Integer.parseInt(epsgFrom.split(":")[1]);
			
	        int idx = shapefile.lastIndexOf(File.separatorChar);
	        String path = shapefile.substring(0, idx + 1); // ie. "/data1/hills.shp" -> "/data1/"
	        String fileName = shapefile.substring(idx + 1); // ie. "/data1/hills.shp" -> "hills.shp"

	        idx = fileName.lastIndexOf(".");

	        if (idx == -1) {
	            throw new Exception("Filename must end in '.shp'");
	        }
	        	        
	        String fileNameWithoutExtention = fileName.substring(0, idx); // ie. "hills.shp" -> "hills"
	        String dbfFileName = path + fileNameWithoutExtention + ".dbf";
				        	        
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
	        	        
	        geoBBox[0] = Double.MAX_VALUE;
			geoBBox[1] = Double.MAX_VALUE;
			geoBBox[2] = -Double.MAX_VALUE;
			geoBBox[3] = -Double.MAX_VALUE;
	        
	        /*Gets the bbox and converts to internal CRS*/
	        
	        /*Envelope bounds = mainHeader.getBounds();
	        
	        Coordinate coord1 = new Coordinate(bounds.getMinX(), bounds.getMinY());
	        
	        Coordinate coord2 = new Coordinate(bounds.getMaxX(), bounds.getMaxY());
	        	        
	        if (epsgFromCode == 4326) {
		    	
	        	final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
		        webMercator.setDatum("WGS84");
	            webMercator.LatLongToWebMercator(coord1);
	            webMercator.LatLongToWebMercator(coord2);
	            
	    	} else if (((epsgFromCode >= 32601) && (epsgFromCode <= 32660)) || ((epsgFromCode >= 32701) && (epsgFromCode <= 32760))) {
	    	
	    		final int utmZone = (epsgFromCode>32700) ? epsgFromCode-32700 : epsgFromCode-32600;
				final boolean southern = (epsgFromCode>32700) ? true : false;
		    
				final UTMLatLongConverter utm = new UTMLatLongConverter();
				utm.setDatum("WGS84");
								
				final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
		        webMercator.setDatum("WGS84");
				
	            utm.UTMToLatLong(coord1, utmZone, southern);
				webMercator.LatLongToWebMercator(coord1);
	            	                	  		
				utm.UTMToLatLong(coord2, utmZone, southern);
				webMercator.LatLongToWebMercator(coord2);
				
	    	}
	        
	        geoBBox[0] = coord1.x;
	        geoBBox[1] = coord1.y;
	        geoBBox[2] = coord2.x;
	        geoBBox[3] = coord2.y;*/
	        
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
            @SuppressWarnings("unused")
	        int recordNumber=0;
	        int contentLength=0;

	        int index = 0;
	        
	        try {
	            while (true) {
	            	
	            	StringBuffer s = mydbf.GetDbfRec(index);
	            	
	            	List<String> attributeNames = new ArrayList<String>();
	            	
	            	recordNumber=file.readIntBE();
	                contentLength=file.readIntBE();                
	                geom = handler.read(file,factory,contentLength);
	
	                //TODO: Should we do it here or in the cluster?
	                //TODO: Maybe it's possible to postpone the conversion and tile computation to the cluster
	                
	                if (epsgFromCode == 4326) {
				    	
			    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
						webMercator.setDatum("WGS84");
			    					    		
			    		geom.apply(new CoordinateFilter() {
			                public void filter(Coordinate coord) {
			                	webMercator.LatLongToWebMercator(coord);
			                }
			            });
						
			    	} else if (((epsgFromCode >= 32601) && (epsgFromCode <= 32660)) || ((epsgFromCode >= 32701) && (epsgFromCode <= 32760))) {
			    	
			    		final int utmZone = (epsgFromCode>32700) ? epsgFromCode-32700 : epsgFromCode-32600;
						final boolean southern = (epsgFromCode>32700) ? true : false;
				    
						final UTMLatLongConverter utm = new UTMLatLongConverter();
						utm.setDatum("WGS84");
						
						final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
						webMercator.setDatum("WGS84");
						
						geom.apply(new CoordinateFilter() {
			                public void filter(Coordinate coord) {
			                	utm.UTMToLatLong(coord, utmZone, southern);
						    	webMercator.LatLongToWebMercator(coord);
			                }
			            });
							                	  		
			    	}
	                					
					geom.setSRID(3857);					
					geom.geometryChanged();
	                
					/*Computing global bounding box*/
					
					double[] bbox = new double[] {geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxX(), geom.getEnvelopeInternal().getMaxY()};
										
					if (bbox[0] < geoBBox[0]) {	//west
						geoBBox[0] = bbox[0];
					}
					
					if (bbox[1] < geoBBox[1]) {	//south
						geoBBox[1] = bbox[1];
					}
					
					if (bbox[2] > geoBBox[2]) {	//east
						geoBBox[2] = bbox[2];
					}
					
					if (bbox[3] > geoBBox[3]) {	//north
						geoBBox[3] = bbox[3];
					}
					
	                //TODO: fix EVERYTHING to work with more than one tile	                
					List<Long> tiles = tileManager.getTiles(bbox);
					
					String tileString = new String();
					
					boolean first = true;
					for (Long i : tiles) {
						if (first) {
							tileString = Long.toString(i);
							first = false;
						} else {
							tileString = tileString + "," + i;
						}
					}
	                					
	                //TODO: Should work with wkb
	                String str = "{\"geometry\":";	                
	                str += "\"" + geom.toText() + "\"";
	                str += ",\"data\":{\"0\":\"\"}";
	                str += ",\"properties\":{\"tile\":\"" + tileString + "\"";
	                
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
	                	                
	                str += ",\"epsg\":";
	                str += "\"" + epsgFrom + "\"";
	                	                	    		    
	    		    String id = new UUID(null).random();	        
	    		    	                
	                str += ",\"iiuuid\":";
	               	str += "\"" + id + "\"";
	                	                
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
			e.printStackTrace();
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
	@SuppressWarnings({ "rawtypes", "unchecked"})
	public static void JSONToShapefile(String json, String shpFileName, List<String> names, boolean keep, String epsgTo) {
	
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
			
			int epsgToCode = Integer.parseInt(epsgTo.split(":")[1]);
			
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
	        
	        boundsArr[0] = Double.MAX_VALUE;
	        boundsArr[1] = Double.MAX_VALUE;
	        boundsArr[2] = -Double.MAX_VALUE;
	        boundsArr[3] = -Double.MAX_VALUE;
	        
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
	        			
	        			if (epsgToCode == 4326) {
	    			    	
	    		    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
	    					webMercator.setDatum("WGS84");
	    		    					    		
	    					geometry.apply(new CoordinateFilter() {
	    		                public void filter(Coordinate coord) {
	    		                	webMercator.WebMercatorToLatLong(coord);
	    		                }
	    		            });
	    					
	    		    	} else if (((epsgToCode >= 32601) && (epsgToCode <= 32660)) || ((epsgToCode >= 32701) && (epsgToCode <= 32760))) {
	    		    				    
	    					final UTMLatLongConverter utm = new UTMLatLongConverter();
	    					utm.setDatum("WGS84");
	    					
	    					final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
	    					webMercator.setDatum("WGS84");
	    					
	    					geometry.apply(new CoordinateFilter() {
	    		                public void filter(Coordinate coord) {
	    		                	webMercator.WebMercatorToLatLong(coord);
	    		                	utm.LatLongToUTM(coord);							    	
	    		                }
	    		            });
	    						                	  		
	    		    	}
	                    					
	        			geometry.setSRID(epsgToCode);					
	        			geometry.geometryChanged();
	        			
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
	        				
	        					if (!columnName.equals("tile")) {
	        					
			        				try {
				                		//Tests if it's an integer
				                	    @SuppressWarnings("unused")
										Integer intValue = Integer.parseInt(value);
				                	    fieldDefs.add(new DbfFieldDef(columnName, 'N', 16, 0));
				                	} catch (NumberFormatException nfe) {
				                	    //Not an integer. Tests if it's a double
				                		try {
				                			@SuppressWarnings("unused")
											Double doubleValue = Double.parseDouble(value);
				                			fieldDefs.add(new DbfFieldDef(columnName, 'F', 33, 16));
				                		} catch (NumberFormatException nfe2) {
				                			//Not a double. Assuming it's a string
				                			fieldDefs.add(new DbfFieldDef(columnName, 'C', 255, 0));		                				
				                		}
				                	}
			        				
	        					} else {
	        						fieldDefs.add(new DbfFieldDef(columnName, 'C', 255, 0));
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
	        			
	        			if (epsgToCode == 4326) {
					    	
				    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
							webMercator.setDatum("WGS84");
				    					    		
							geometry.apply(new CoordinateFilter() {
				                public void filter(Coordinate coord) {
				                	webMercator.WebMercatorToLatLong(coord);
				                }
				            });
							
				    	} else if (((epsgToCode >= 32601) && (epsgToCode <= 32660)) || ((epsgToCode >= 32701) && (epsgToCode <= 32760))) {
				    						    
							final UTMLatLongConverter utm = new UTMLatLongConverter();
							utm.setDatum("WGS84");
							
							final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
							webMercator.setDatum("WGS84");
							
							geometry.apply(new CoordinateFilter() {
				                public void filter(Coordinate coord) {
				                	webMercator.WebMercatorToLatLong(coord);
				                	utm.LatLongToUTM(coord);							    	
				                }
				            });
								                	  		
				    	}
		                					
	        			geometry.setSRID(epsgToCode);					
	        			geometry.geometryChanged();
	        			
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
		        				
		        				if (!columnName.equals("tile")) {
		        				
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
			        				
		        				} else {
		        					DBFrow.add(value);
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
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Converts from Shapefile to WKT. This method also converts the reference system.<br>
	 * @param shapefile - shapefile path<br>
	 * wkt - WKT file path<br>
	 * epsgFrom - input EPSG code in the form "EPSG:0000"
	 */	
	@SuppressWarnings("unused")
	public static void shapefileToWKT(String shapefile, String wkt, String epsgFrom) {
		
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
			
			int epsgFromCode = Integer.parseInt(epsgFrom.split(":")[1]);
			
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
	                
	                //TODO: Should we do it here or in the cluster?
	                //TODO: Maybe it's possible to postpone the conversion and tile computation to the cluster
	                
	                if (epsgFromCode == 4326) {
				    	
			    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
						webMercator.setDatum("WGS84");
			    					    		
			    		geom.apply(new CoordinateFilter() {
			                public void filter(Coordinate coord) {
			                	webMercator.LatLongToWebMercator(coord);
			                }
			            });
						
			    	} else if (((epsgFromCode >= 32601) && (epsgFromCode <= 32660)) || ((epsgFromCode >= 32701) && (epsgFromCode <= 32760))) {
			    	
			    		final int utmZone = (epsgFromCode>32700) ? epsgFromCode-32700 : epsgFromCode-32600;
						final boolean southern = (epsgFromCode>32700) ? true : false;
				    
						final UTMLatLongConverter utm = new UTMLatLongConverter();
						utm.setDatum("WGS84");
						
						final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
						webMercator.setDatum("WGS84");
						
						geom.apply(new CoordinateFilter() {
			                public void filter(Coordinate coord) {
			                	utm.UTMToLatLong(coord, utmZone, southern);
						    	webMercator.LatLongToWebMercator(coord);
			                }
			            });
							                	  		
			    	}
	                					
					geom.setSRID(3857);					
					geom.geometryChanged();
	                
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
	public static void WKTToShapefile(String wkt, String shpFileName, String epsgTo) {
		
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
			
			int epsgToCode = Integer.parseInt(epsgTo.split(":")[1]);
			
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
	        
	        boundsArr[0] = Double.MAX_VALUE;
	        boundsArr[1] = Double.MAX_VALUE;
	        boundsArr[2] = -Double.MAX_VALUE;
	        boundsArr[3] = -Double.MAX_VALUE;
	        
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
    			
	        	if (epsgToCode == 4326) {
			    	
		    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
					webMercator.setDatum("WGS84");
		    					    		
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	webMercator.WebMercatorToLatLong(coord);
		                }
		            });
					
		    	} else if (((epsgToCode >= 32601) && (epsgToCode <= 32660)) || ((epsgToCode >= 32701) && (epsgToCode <= 32760))) {
		    				    
					final UTMLatLongConverter utm = new UTMLatLongConverter();
					utm.setDatum("WGS84");
					
					final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
					webMercator.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	webMercator.WebMercatorToLatLong(coord);
		                	utm.LatLongToUTM(coord);							    	
		                }
		            });
						                	  		
		    	}
                					
    			geometry.setSRID(epsgToCode);					
    			geometry.geometryChanged();
	        	
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
    			
    			if (epsgToCode == 4326) {
			    	
		    		final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
					webMercator.setDatum("WGS84");
		    					    		
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	webMercator.WebMercatorToLatLong(coord);
		                }
		            });
					
		    	} else if (((epsgToCode >= 32601) && (epsgToCode <= 32660)) || ((epsgToCode >= 32701) && (epsgToCode <= 32760))) {
		    				    
					final UTMLatLongConverter utm = new UTMLatLongConverter();
					utm.setDatum("WGS84");
					
					final WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
					webMercator.setDatum("WGS84");
					
					geometry.apply(new CoordinateFilter() {
		                public void filter(Coordinate coord) {
		                	webMercator.WebMercatorToLatLong(coord);
		                	utm.LatLongToUTM(coord);							    	
		                }
		            });
						                	  		
		    	}
                					
    			geometry.setSRID(epsgToCode);					
    			geometry.geometryChanged();
    			
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
