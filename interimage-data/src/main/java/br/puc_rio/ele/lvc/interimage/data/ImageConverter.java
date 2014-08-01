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

package br.puc_rio.ele.lvc.interimage.data;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Exception;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import br.puc_rio.ele.lvc.interimage.common.TileManager;

/**
 * Converts between an image format and InterIMAGE formats.<br>
 * 
 * {@linkplain http://www.lvc.ele.puc-rio.br/projects/interimage/wiki/index.php/Data_Package}
 * 
 * In particular, here are the checks done in order:
 * TODO Implement Interimage generic data
 * TODO Extend to multidimensions (3D, 4D, 5D)
 * TODO Use different types instead of DoubleType;
 * TODO Iterate over only valid data ('No Data') - use a ROI? 
 * TODO Use classes with Template!
 * @author Patrick Happ, Rodrigo Ferreira
 *
 */

public class ImageConverter {
	
	public static void ImageToJSON(Image imageObj, String imagePath, List<String> list, boolean keep, TileManager tileManager) {
		
		try {
			
			String image = imageObj.getURL();
			
			double[] geoBBox = new double[] {imageObj.getGeoWest(), imageObj.getGeoSouth(), imageObj.getGeoEast(), imageObj.getGeoNorth()};
			
			/* Processing input parameters */
			if (image == null) {
	            throw new Exception("No Image specified");
	        } else {
	            if (image.isEmpty()) {
	            	throw new Exception("No Image specified");
	            }
	        }
			
			/*Treating format names*/
            
            int idx = image.lastIndexOf(".");
            
            String extension = image.substring(idx);
            String formatName = null;
            
            if ((extension.equals(".tif")) || (extension.equals(".tiff"))) {
            	formatName = "tif";
            } else if ((extension.equals(".jpg")) || (extension.equals(".jpeg")) || (extension.equals(".jls")) || (extension.equals(".jfif"))) {
            	formatName = "jpg";
            } else if (extension.equals(".png")) {
            	formatName = "png";
            } else if (extension.equals(".jp2")) {
            	formatName = "jpeg2000";
            } else if (extension.equals(".bmp")) {
            	formatName = "bmp";
            } else if ((extension.equals(".pbm")) || (extension.equals(".pgm")) || (extension.equals(".ppm"))) {
            	formatName = "pnm";
            }
			
            if (formatName == null) {
            	throw new Exception("Image format not supported: " + extension);
            }
            
			/*if (json == null) {
	            throw new Exception("No JSON specified");
	        } else {
	            if (json.isEmpty()) {
	            	throw new Exception("No JSON specified");
	            }
	        }*/
			
	        /*int idx = image.lastIndexOf(File.separatorChar);
	        String path = image.substring(0, idx + 1); // ie. "/data1/hills.tiff" -> "/data1/"
	        String fileName = image.substring(idx + 1); // ie. "/data1/hills.tiff" -> "hills.tiff"

	        idx = fileName.lastIndexOf(".");

	        if (idx == -1) {
	            throw new Exception("Filename must contains the format: '.tiff', '.jpeg'... ");
	        }
	        
	        String fileNameWithoutExtention = fileName.substring(0, idx); // ie. "hills.tiff" -> "hills"
	        			
	        if (json.isEmpty()) {
	        	json = path + fileNameWithoutExtention + ".json";
	        }*/
	        
	        
	        /* Stream for output file */
			//OutputStream out = new FileOutputStream(json);		
	        
			InputStream input = new FileInputStream(image);
			
	        /* Preparing to read the image */
	        ImageInputStream in = ImageIO.createImageInputStream(input); 
	        /* try to decode the image with all registered imagereaders */
	        Iterator<ImageReader> readers = ImageIO.getImageReaders(in);
	        
	        ImageReader reader = null;
	        
	        if (readers.hasNext()) {
	            reader = readers.next();
	            reader.setInput(in);           
	        } else {
	        	/* TODO: Test if it is supported format */
	        	//out.close();
		        throw new Exception("Unsuported image type");
	        }
	        
	        ImageReadParam param = reader.getDefaultReadParam();
	        
	        int imgW = reader.getWidth(0);
	        int imgH = reader.getHeight(0);
	        	        
	        int numTilesX = tileManager.getNumTilesX();
	        
	        double tileSize = tileManager.getTileSize();
	        
	        double[] worldBBox = tileManager.getWorldBBox();
	        
	        int[] tiles = tileManager.getTileCoordinates(geoBBox);
	        
	        Rectangle sourceRegion = new Rectangle();    		
	        
    		BufferedImage img=null;
    		            
            /*deleting previous files*/
            File dir = new File(imagePath);
            
            dir.mkdirs();
            
            for (final File fileEntry : dir.listFiles()) {
		        if (fileEntry.isDirectory()) {
		        	//ignore
		        } else {
		        	fileEntry.delete();
		        }
		    }
    		
	        for (int j=tiles[3]; j>=tiles[1]; j--) {
	        	for (int i=tiles[0]; i<=tiles[2]; i++) {
	        		
					long id = ((long)j)*numTilesX+i+1;
	        		
					//System.out.print(id);
					
	        		double[] geoTile = new double[4];
	        		
	        		geoTile[0] = i*tileSize + worldBBox[0];
	        		geoTile[1] = j*tileSize + worldBBox[1];
	        		geoTile[2] = geoTile[0] + tileSize;
	        		geoTile[3] = geoTile[1] + tileSize;
	        		
	        		/*System.out.println();
	        		
	        		System.out.println(geoTile[0]);
	        		System.out.println(geoTile[1]);
	        		System.out.println(geoTile[2]);
	        		System.out.println(geoTile[3]);
	        		
	        		System.out.println();	        		
	        		
	        		System.out.println(geoBBox[0]);
	        		System.out.println(geoBBox[1]);
	        		System.out.println(geoBBox[2]);
	        		System.out.println(geoBBox[3]);*/
	        		
	        		/*geo intersection*/
	        		double[] srGeoBox = new double[4];
	        		
	        		//System.out.println();
	        		
	        		srGeoBox[0] = Math.max(geoTile[0], geoBBox[0]);
	        		srGeoBox[1] = Math.max(geoTile[1], geoBBox[1]);
	        		srGeoBox[2] = Math.min(geoTile[2], geoBBox[2]);
	        		srGeoBox[3] = Math.min(geoTile[3], geoBBox[3]);
	        		
	        		/*System.out.println();
	        		
	        		System.out.println(srGeoBox[0]);
	        		System.out.println(srGeoBox[1]);
	        		System.out.println(srGeoBox[2]);
	        		System.out.println(srGeoBox[3]);*/
	        		
	        		int[] imgBBox = Image.imgBBox(srGeoBox, geoBBox, new int[] {imgW, imgH});
	        		
	        		/*System.out.println();
	        		
	        		System.out.println(imgBBox[0]);
	        		System.out.println(imgBBox[1]);
	        		System.out.println(imgBBox[2]);
	        		System.out.println(imgBBox[3]);*/
	        		
	        		/* Set The region to extract */
	        		sourceRegion.setSize(imgBBox[2]-imgBBox[0]+1,imgBBox[1]-imgBBox[3]+1);//set size
	        		sourceRegion.setLocation(imgBBox[0], imgBBox[3]);//set origin
	                param.setSourceRegion(sourceRegion);
	                
	                // Will read only the region specified
	                if (img != null)
	                	img.flush();
	                
	                img = reader.read(0, param);
	             	                
	                /*Write tiff file*/
	                File outputfile = new File(imagePath + "T" + id + extension);
	                
	                ImageIO.write(img, formatName, outputfile);
	                	                
	                /*tfw file just for test purposes*/
	                //OutputStream out = new FileOutputStream(imagePath + "T" + id + extension + "w");
	                
	                double[] newGeo = Image.geoBBox(imgBBox, geoBBox, new int[] {imgW, imgH});
	                
	                //double resX = (geoBBox[2]-geoBBox[0])/imgW;
	                //double resY = (geoBBox[1]-geoBBox[3])/imgH;
	                
	                /*System.out.println();
	                
	                System.out.println(newGeo[0]);
	        		System.out.println(newGeo[1]);
	        		System.out.println(newGeo[2]);
	        		System.out.println(newGeo[3]);*/
	        			                
	                /*String str = resX + "\n";
	                str = str + 0.0 + "\n";
	                str = str + 0.0 + "\n";
	                str = str + resY + "\n";
	                str = str + (newGeo[0]+(resX/2)) + "\n";
	                str = str + (newGeo[3]+(resY/2)) + "\n";
	                
	                out.write(str.getBytes());
	                
	                out.close();*/
	                
	                OutputStream out3 = new FileOutputStream(imagePath + "T" + id + ".meta");
	                
	                String str = imageObj.getBands() + "\n";
	                str = str + (imgBBox[2]-imgBBox[0]+1) + "\n";
	                str = str + (imgBBox[1]-imgBBox[3]+1) + "\n";
	                str = str + newGeo[0] + "\n";
	                str = str + newGeo[1] + "\n";
	                str = str + newGeo[2] + "\n";
	                str = str + newGeo[3] + "\n";
	                
	                out3.write(str.getBytes());
	                
	                out3.close();
	                
	                /*OutputStream[] bandFiles = new FileOutputStream[img.getData().getNumDataElements()];
	                
	                for (int b=0; b<imageObj.getBands(); b++) {
	                
	                	//Write json file
    	                bandFiles[b] = new FileOutputStream(projectPath + "images/" + imageObj.getKey() + "_" + b + "_T" + id + ".json");
    	             
    	                String str = img.getWidth() + " " + img.getHeight();
    	                
    	                bandFiles[b].write(str.getBytes());
    	                
	                }
	                	
	                boolean first = true;
	                for (int y=0; y<img.getHeight(); y++) {
	                	for (int x=0; x<img.getWidth(); x++) {
	                		
	                		double[] data = null;
	                		
	                		data = img.getData().getPixel(x, y, data);
	                		
	                		for (int k=0; k<img.getData().getNumDataElements(); k++) {
	                			String str = null;
	                			if (first) {
	                				str = String.valueOf(data[k]);	                				
	                			} else {
	                				str = " " + data[k];
	                			}
	                			bandFiles[k].write(str.getBytes());
	                		}
	                		
	                		if (first)
	                			first = false;
	                				    	                
	                	}
	                }*/
	                	                	                	        		
	        	}
	        }
	      	            
            /*OutputStream out2 = new FileOutputStream(projectPath + imageObj.getKey() + ".meta");
                		
            String str = imageObj.getBands() + "\n";
            str = str + imgW + "\n";
            str = str + imgH + "\n";
            str = str + geoBBox[0] + "\n";
            str = str + geoBBox[1] + "\n";
            str = str + geoBBox[2] + "\n";
            str = str + geoBBox[3] + "\n";
            
            out2.write(str.getBytes());
            
            out2.close();*/	        
	        
	        in.close();
	        //out.close();
	               
		} catch (Exception e) {
			System.err.println("Failed to parse image; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}	
}