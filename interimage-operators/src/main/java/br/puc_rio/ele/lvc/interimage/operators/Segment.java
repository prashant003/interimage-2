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

package br.puc_rio.ele.lvc.interimage.operators;

/**
 * An image object or segment<br>
 * 
 * 
 * @author Patrick Happ
 *
 */

public final class Segment {
	 private int  id; /* segment id */
	  private double area=1; /* number of pixels in segment */
	  private double perimeter=4; /*number of border pixels */
	  private double b_box[] = {0,0,1,1}; /* bounding box of the segment, relative to rows and cols */
	  private double avg_color[]; /* average colors of pixels, one for each band */
	  private double std_color[]; /* std of pixel colors, one for each band */
	  private double avg_color_square[];
	  private double color_sum[];
	  private boolean used = false; /* indicate if segment has been used in segmentation step */
	  
	  //HashMap <Long, Boolean> pixelList = new HashMap <Long, Boolean>();
	  private Pixel pixel_list; /* list of indexes of the segment's pixels */
	  private Pixel last_pixel; /* pointer to the last pixel in the pixel list */
	  
	  public int getId() {
		  return id;
	  }


		public void setId(int id) {
			this.id = id;
		}
		
		
		public double getArea() {
			return area;
		}
		
		
		public void setArea(double area) {
			this.area = area;
		}
		
		
		public double getPerimeter() {
			return perimeter;
		}
		
		
		public void setPerimeter(double perimeter) {
			this.perimeter = perimeter;
		}
		
		
		public double[] getB_box() {
			return b_box;
		}
			
		public void setB_box(double[] b_box) {
			this.b_box = b_box;
		}
		
		public double getB_boxByIndex(int idx) {
			return b_box[idx];
		}
		
		public void setB_boxByIndex(double b_box, int idx) {
			this.b_box[idx] = b_box;
		}
		
		public double[] getAvg_color() {
			return avg_color;
		}
		
		
		public void setAvg_color(double[] avg_color) {
			this.avg_color = avg_color;
		}
		
		
		public double[] getStd_color() {
			return std_color;
		}
		
		
		public void setStd_color(double[] std_color) {
			this.std_color = std_color;
		}
		
		
		public double[] getAvg_color_square() {
			return avg_color_square;
		}
		
		
		public void setAvg_color_square(double[] avg_color_square) {
			this.avg_color_square = avg_color_square;
		}
		
		
		public double[] getColor_sum() {
			return color_sum;
		}
		
		
		public void setColor_sum(double[] color_sum) {
			this.color_sum = color_sum;
		}
		
		
		public boolean isUsed() {
			return used;
		}
		
		
		public void setUsed(boolean used) {
			this.used = used;
		}
		
		
		public Pixel getPixel_list() {
			return pixel_list;
		}
		
		
		public void setPixel_list(Pixel pixel_list) {
			this.pixel_list = pixel_list;
		}
		
		
		public Pixel getLast_pixel() {
			return last_pixel;
		}
		
		
		public void setLast_pixel(Pixel last_pixel) {
			this.last_pixel = last_pixel;
		}
		
		public void createSpectral(int nBands) {
			this.avg_color = new double[nBands];
			this.std_color = new double[nBands];
			this.avg_color_square = new double[nBands];
			this.color_sum = new double[nBands];
		}
		
		public double getAvg_colorByIndex(int idx) {
			return avg_color[idx];
		}
		
		public void setAvg_colorByIndex(double val, int idx) {
			this.avg_color[idx] = val;
		}
		
		public double getStd_colorByIndex(int idx) {
			return std_color[idx];
		}
		
		public void setStd_colorByIndex(double val, int idx) {
			this.std_color[idx] = val;
		}
		
		public double getAvg_color_squareByIndex(int idx) {
			return avg_color_square[idx];
		}
		
		public void setAvg_color_squareByIndex(double val, int idx) {
			this.avg_color_square[idx] = val;
		}
		
		public double getColor_sumByIndex(int idx) {
			return color_sum[idx];
		}
		
		public void setColor_sumByIndex(double val, int idx) {
			this.color_sum[idx] = val;
		}


		private  Segment (int id) {
		     this.id = id;
		 }
	  
	  
		public static Segment create (int id) {
		     return new Segment (id);
		 }
	
}

