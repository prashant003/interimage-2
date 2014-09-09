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

public final class Pixel {

	private int  id; /* pixel id */
	private boolean borderline; /* indicates if the pixel belongs to the border of the segment */
	private Pixel next_pixel; /* next pixel */
	
	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public boolean isBorderline() {
		return borderline;
	}


	public void setBorderline(boolean borderline) {
		this.borderline = borderline;
	}


	public Pixel getNext_pixel() {
		return next_pixel;
	}


	public void setNext_pixel(Pixel next_pixel) {
		this.next_pixel = next_pixel;
	}

	
	private Pixel (int id) {
		this.id = id;
	}
	  
	  
	public static Pixel create (int id) {
		return new Pixel (id);
	}
	
}