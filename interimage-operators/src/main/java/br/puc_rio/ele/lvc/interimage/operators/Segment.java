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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * An image object or segment<br>
 * 
 * 
 * @author Patrick Happ
 *
 */

public class Segment {
	//TODO: make it generic? Can use a Map of attributes
	private int segment_id;			// Segment id
	private double  area;				// Area
	//int []  b_box;				// standard bounding box
	//TODO: change to private and create getter and setter
	public double[] avg_color;			// Average color
	public double[] std_color;			// Standard Color
	public double[]  avg_square_color;	// Average Square Color
	public double[]  sum_color;			// Sum Color
	
	private double mean_x;
	private double mean_y;
	private long  sum_x;
	private long sum_y;
	private long  square_x;
	private long square_y;
	private long  product_xy;
	private double bb_length;
	private double bb_width;
	
	private HashMap<Integer, Pixel> pixelList;   	// Best neighbour's id
	private double fusion_f;			   // Fusion Factor for merge best neighbor
	
	//Segment next_seg=null;	//Next Segment on list
	//Segment prev_seg=null;  //Previous Segment on list
	
	//Best Neighbor List
	private HashMap<Integer, Segment> bestNeighbor;
	private Set<Integer> neighborIds;
	
	public Segment(int id){
		setSegment_id(id);
		setPixelList(new HashMap<Integer, Pixel>());
		Pixel px = new Pixel(id);
		getPixelList().put(id, px);
		resetSegment();
	}
	
	public void resetSegment(){
		//spatial attributes
		setArea(1);
		setBb_length(1);
		setBb_width(1);
		
		//b_box = new int[4];
		setNeighborIds(new HashSet<Integer>());
		setBestNeighbor(new HashMap<Integer, Segment>());
		setFusion_f(Double.MAX_VALUE);

		resetBestNeighbor();
	}
	
	public void resetBestNeighbor(){
		getBestNeighbor().clear();
		setFusion_f(Double.MAX_VALUE);
	}
	
	public void Kill(){
		getBestNeighbor().clear();
		getPixelList().clear();
		getNeighborIds().clear();
		
		setPixelList(null);
		//b_box=null;
		setNeighborIds(null);
		setBestNeighbor(null);
		setPixelList(null);
		avg_color=null;
		std_color=null;
		avg_square_color=null;
		sum_color=null;
	}
		
	public void setNumBands(int nBands){
		avg_color = new double[nBands];
		std_color = new double[nBands];
		avg_square_color = new double[nBands];
		sum_color = new double[nBands];
	}
	
	public void reset_border(int width, int height )
	{
		int [] neighbor_pixels_id;
		// for each pixel of the new segment
		for (Pixel aux_pixel : getPixelList().values()) {
			if (aux_pixel.isBorder()){
				neighbor_pixels_id = aux_pixel.getPixelIdFromNeighbors(width, height);
				boolean isBorder=false;
				
				for (int i=0;i<4;i++)
				{
					if (neighbor_pixels_id[0]== -1) //image limit 
					{
						isBorder=true;
						break;
					}
					else
					{
						if (!getPixelList().containsKey(neighbor_pixels_id[i])){
							isBorder=true;
							break;
						}
					}
				}
				aux_pixel.setBorder(isBorder);			
			}
		}
	}

	public double getMean_x() {
		return mean_x;
	}

	public void setMean_x(double mean_x) {
		this.mean_x = mean_x;
	}

	public double getMean_y() {
		return mean_y;
	}

	public void setMean_y(double mean_y) {
		this.mean_y = mean_y;
	}

	public long getSum_x() {
		return sum_x;
	}

	public void setSum_x(long sum_x) {
		this.sum_x = sum_x;
	}

	public long getSum_y() {
		return sum_y;
	}

	public void setSum_y(long sum_y) {
		this.sum_y = sum_y;
	}

	public long getSquare_x() {
		return square_x;
	}

	public void setSquare_x(long square_x) {
		this.square_x = square_x;
	}

	public long getSquare_y() {
		return square_y;
	}

	public void setSquare_y(long square_y) {
		this.square_y = square_y;
	}

	public long getProduct_xy() {
		return product_xy;
	}

	public void setProduct_xy(long product_xy) {
		this.product_xy = product_xy;
	}
	
	//public int getBBoxbyIndex(int idx) {
	//	return b_box[idx];
	//}
	//
	//public void setBBoxbyIndex(int idx, int val) {
	//	this.b_box[idx] = val;
	//}

	public int getSegment_id() {
		return segment_id;
	}

	public void setSegment_id(int segment_id) {
		this.segment_id = segment_id;
	}

	public HashMap<Integer, Pixel> getPixelList() {
		return pixelList;
	}

	public void setPixelList(HashMap<Integer, Pixel> pixelList) {
		this.pixelList = pixelList;
	}

	public Set<Integer> getNeighborIds() {
		return neighborIds;
	}

	public void setNeighborIds(Set<Integer> neighborIds) {
		this.neighborIds = neighborIds;
	}

	public HashMap<Integer, Segment> getBestNeighbor() {
		return bestNeighbor;
	}

	public void setBestNeighbor(HashMap<Integer, Segment> bestNeighbor) {
		this.bestNeighbor = bestNeighbor;
	}

	public double getArea() {
		return area;
	}

	public void setArea(double area) {
		this.area = area;
	}

	public double getFusion_f() {
		return fusion_f;
	}

	public void setFusion_f(double fusion_f) {
		this.fusion_f = fusion_f;
	}

	public double getBb_width() {
		return bb_width;
	}

	public void setBb_width(double bb_width) {
		this.bb_width = bb_width;
	}

	public double getBb_length() {
		return bb_length;
	}

	public void setBb_length(double bb_length) {
		this.bb_length = bb_length;
	}
	
}

