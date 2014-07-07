/* OpenCarto - Spatial data transformation, multi-scale mapping, vector web mapping

Copyright (C) 2014  Julien Gaffuri

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/

package br.puc_rio.ele.lvc.interimage.geometry;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * 
 * @author julien Gaffuri
 * http://sourceforge.net/p/opencarto/code/HEAD/tree/trunk/server/src/main/java/org/opencarto/algo/base/
 *
 */
public class SmallestSurroundingRectangle {
	//public static Logger logger = Logger.getLogger(SmallestSurroundingRectangle.class.getName());

	public static Polygon get(Geometry geom){
		return get(geom, geom.getFactory());
	}

	public static Polygon get(Geometry geom, GeometryFactory gf){
		Geometry hull_ = (new ConvexHull(geom)).getConvexHull();
		if (!(hull_ instanceof Polygon)) return null;
		Polygon convHull = (Polygon)hull_;

		Coordinate c = geom.getCentroid().getCoordinate();
		Coordinate[] coords = convHull.getExteriorRing().getCoordinates();

		double minArea = Double.MAX_VALUE, minAngle = 0.0;
		Polygon ssr = null;
		Coordinate ci = coords[0], cii;
		for(int i=0; i<coords.length-1; i++){
			cii = coords[i+1];
			double angle = Math.atan2(cii.y-ci.y, cii.x-ci.x);
			Polygon rect = (Polygon) Rotation.get(convHull, c, -1.0*angle, gf).getEnvelope();
			double area = rect.getArea();
			if (area < minArea) {
				minArea = area;
				ssr = rect;
				minAngle = angle;
			}
			ci = cii;
		}
		return Rotation.get(ssr, c, minAngle, gf);
	}

	public static Polygon get(Geometry geom, boolean preserveSize){
		return get(geom, geom.getFactory(), preserveSize);
	}

	public static Polygon get(Geometry geom, GeometryFactory gf, boolean preserveSize){
		if( !preserveSize ) return get(geom, gf);

		Polygon out = get(geom, gf);
		double ini = geom.getArea();
		double fin = out.getArea();

		if(fin == 0) {
			//logger.warning("Failed to preserve size of smallest surrounding rectangle: Null final area.");
			return out;
		}

		return Scaling.get(out, out.getCentroid().getCoordinate(), Math.sqrt(ini/fin), gf);
	}

}
