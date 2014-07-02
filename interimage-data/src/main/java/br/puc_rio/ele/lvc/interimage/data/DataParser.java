package br.puc_rio.ele.lvc.interimage.data;

import java.lang.Exception;
import java.util.ArrayList;
import java.util.List;

import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.Cursor;
import net.imglib2.type.numeric.real.DoubleType;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;


/**
 * Retrieves an image layer from a pig attribute. It detects the type of the column
 * and the data stored in that column and automatically detects its format
 * and tries to get the image layer from it. 
 * It understands a generic format.
 * 
 * {@linkplain http://www.lvc.ele.puc-rio.br/projects/interimage/wiki/index.php/Data_Package}
 * 
 * In particular, here are the checks done in order:
 * TODO Implement Interimage generic data
 * TODO Extend to multidimensions (3D, 4D, 5D)
 * TODO Use different types instead of DoubleType;
 * TODO Iterate over only valid data ('No Data') - use a ROI? 
 * @author Patrick Happ
 * @author Rodrigo Ferreira
 *
 */

public class DataParser {


	public Img< DoubleType > parseData(Object objImage) {
		
		try {
			//Assuming generic data in a json file
			String strImage = (String)objImage;
			String[] tokens = strImage.trim().split("\\s");
			int type = Integer.parseInt(tokens[0]);
			//TODO using cellnumber for CellImg --> performance
			int width = Integer.parseInt(tokens[1]);
			int height = Integer.parseInt(tokens[2]);
			//TODO maxValue will not be used? It can be defined in type
			//int maxValue = Integer.parseInt(tokens[3]);
			
			Img <DoubleType> ip = null;
			//TODO Different executions depending of type 
			switch  (type) {
				case 0:
					final ImgFactory< DoubleType > factory = new ArrayImgFactory< DoubleType  >();
					ip = factory.create(new long[] {width,height}, new DoubleType());
				break;
				default:
					System.out.println("Image Type is not recognized!");
			} 

			Cursor < DoubleType > cursor = ip.cursor();
			int i=3;
			while (cursor.hasNext() )
	        {
				DoubleType pixelValue = cursor.next();
				pixelValue.set( Double.parseDouble(tokens[i]) );
				i=i+1;
	        }
			return ip;
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}
	
	public ImageProcessor parseImgProc(Object objImage) {
		
		try {
			//Assuming generic data in a json file
			String strImage = (String)objImage;
			String[] tokens = strImage.trim().split("\\s");
			int type = Integer.parseInt(tokens[0]);
			int width = Integer.parseInt(tokens[1]);
			int height = Integer.parseInt(tokens[2]);
			//TODO maxValue will not be used? It can be defined in type
			//int maxValue = Integer.parseInt(tokens[3]);
			ImageProcessor ip = null;
			//TODO Different executions depending of type 
			switch  (type) {
				case 0:
					ip = new ShortProcessor(width, height);
				break;
				default:
					System.out.println("Image Type is not recognized!");
			} 
			
			int imgidx = 0;
			for (int t=3; t<tokens.length; t++) {
				ip.set(imgidx%width, imgidx/width, Integer.parseInt(tokens[t]));
				imgidx++;
			}
			return ip;
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}
	
	public List< Double > parseListData(Object objImage) {
		
		try {
			//Assuming generic data in a json file
			String strImage = (String)objImage;
			String[] tokens = strImage.trim().split("\\s");
			int type = Integer.parseInt(tokens[0]);
			//Do not use width and height
			//int width = Integer.parseInt(tokens[1]);
			//int height = Integer.parseInt(tokens[2]);
				
			//TODO using another kind of array --> performance
			 final List<Double> lst = new ArrayList<Double>();
			//TODO Different executions depending of type 
			switch  (type) {
				case 0:
					for (int i=3; i<tokens.length; i++) {
						Double pixelValue = Double.parseDouble(tokens[i]);
						lst.add(pixelValue);
			        }
				break;
				default:
					System.out.println("Image Type is not recognized!");
			} 

			
			return lst;
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}
	
	/*public FloatProcessor parseGeometryImageData(Map<String, Map<String, Object>> tiles, int band, Image image, Geometry geometry) {

		try {
			
			BufferedImage buff = (BufferedImage)tiles.get("0").get("image");
			double[] geoBox = (double[])tiles.get("0").get("geoBox");
			
			int[] bBox = Image.imgBBox(new double[] {geometry.getEnvelopeInternal().getMinX(), geometry.getEnvelopeInternal().getMinY(), geometry.getEnvelopeInternal().getMaxX(), geometry.getEnvelopeInternal().getMaxY()}, 
					new double[] {image.getGeoWest(), image.getGeoSouth(), image.getGeoEast(), image.getGeoNorth()}, new int[] {buff.getWidth(), buff.getHeight()});
			
			FloatProcessor ip = new FloatProcessor(bBox[2]-bBox[0]+1, bBox[1]-bBox[3]+1);
			
			int imgidx = 0;
			
			ImageProcessor mask = new ShortProcessor(bBox[2]-bBox[0]+1, bBox[1]-bBox[3]+1);
			
			for (int j=0; j<buff.getHeight(); j++) {
				for (int i=0; i<buff.getWidth(); i++) {
					float dArray[] = new float[buff.getRaster().getNumDataElements()];
					buff.getRaster().getPixel(i, j, dArray);
					ip.setf(imgidx%buff.getWidth(), imgidx/buff.getWidth(), dArray[band]);
					imgidx++;					
				}
			}
			
			ip.setMask(mask);
			
			return ip;
			
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
		
	}*/
	
}