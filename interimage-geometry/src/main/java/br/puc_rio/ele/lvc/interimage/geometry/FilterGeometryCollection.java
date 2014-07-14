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

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * A class that filters a geometry collection and keeps only the polygons.
 * @author Rodrigo Ferreira
 */
public class FilterGeometryCollection {

	public static Geometry filter(Geometry gc) {
		
		List<Geometry> list = new ArrayList<Geometry>();
		
		for (int i=0; i<gc.getNumGeometries(); i++) {
			if (gc.getGeometryN(i) instanceof Polygon) {
				list.add(gc.getGeometryN(i));
			}
		}
		
		Geometry[] geoms = new Geometry[list.size()]; 
		
		for (int j=0; j<list.size(); j++) {
			geoms[j] = list.get(j);
		}
		
		return new GeometryCollection(geoms, new GeometryFactory());
	}

}
