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
 * An image pixel<br>
 * 
 * 
 * @author Patrick Happ
 *
 */

//TODO: Xpos and Ypos should be an attribute?

public class Pixel {

	private int _pixel_id;				// Pixel id
	//private int _segment_id;
	//Pixel next_pixel;
	private boolean _pixel_borderline;	// If pixel is border of segment
	
	
	public Pixel(int id){
		_pixel_id=id;
		this.setBorder(true);
	}
	
	/*public void setSegmentId(int id){
		 _segment_id=id;
	}
	 
	public int getSegmentId(){
		 return _segment_id;
	}*/
	
	public int getPixelId(){
		 return _pixel_id;
	}
	
	public void setBorder(boolean val){
		 _pixel_borderline = val;
	}
	
	public int getX(int width){
		 return  _pixel_id % (width);
	}
	
	public int getY(int width){
		 return _pixel_id / width;
	}
	
	public boolean isBorder(){
		 return _pixel_borderline;
	}
	
	public int [] getPixelIdFromNeighbors(int width, int height){
		
		int [] neighb = new int [4];
		for (int i=0; i<4; i++){
			neighb[i]=-1;
		}
		
		int x = this.getX(width);
		int y = this.getY(width);
		
		if (y>0) 
			neighb[0]=(y-1)* width + x; //north
		if (x>0) 
			neighb[1]= y* width + (x-1);//west
		if (y < height-1) 
			neighb[2]=(y+1)* width + x; //south
		if (x < width-1) 
			neighb[3]= y* width + (x+1);//east
		
		return neighb;
	}
	
}

