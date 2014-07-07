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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * 
 * @author julien Gaffuri
 * http://sourceforge.net/p/opencarto/code/HEAD/tree/trunk/server/src/main/java/org/opencarto/algo/base/
 *
 */
public class Scaling {	

	public static Coordinate[] get(Coordinate[] coord, Coordinate c, double coef){
		Coordinate[] coord_= new Coordinate[coord.length];
		double xc = c.x, yc = c.y;
		Coordinate ci;
		double x, y;
		for(int i=0; i<coord.length; i++) {
			ci = coord[i];
			x = ci.x;
			y = ci.y;
			coord_[i] = new Coordinate(xc+coef*(x-xc), yc+coef*(y-yc));
		}
		return coord_;
	}

	public static Point get(Point geom, Coordinate c, double coef, GeometryFactory gf) {
		double xc = c.x, yc = c.y;
		return gf.createPoint( new Coordinate(xc+coef*(geom.getX()-xc), yc+coef*(geom.getY()-yc)) );
	}

	public static LineString get(LineString ls, Coordinate c, double coef, GeometryFactory gf) {
		return gf.createLineString(get(ls.getCoordinates(), c, coef));
	}

	public static LinearRing get(LinearRing lr, Coordinate c, double coef, GeometryFactory gf) {
		return gf.createLinearRing(get(lr.getCoordinates(), c, coef));
	}

	public static Polygon get(Polygon geom, Coordinate c, double coef, GeometryFactory gf) {
		LinearRing lr = get((LinearRing)geom.getExteriorRing(), c, coef, gf);
		LinearRing[] lr_ = new LinearRing[geom.getNumInteriorRing()];
		for(int j=0; j<geom.getNumInteriorRing(); j++) lr_[j] = get((LinearRing)geom.getInteriorRingN(j), c, coef, gf);
		return gf.createPolygon(lr, lr_);
	}

	public static GeometryCollection get(GeometryCollection geomCol, Coordinate c, double coef, GeometryFactory gf) {
		Geometry[] gs = new Geometry[geomCol.getNumGeometries()];
		for(int i=0; i< geomCol.getNumGeometries(); i++) gs[i] = get(geomCol.getGeometryN(i), c, coef, gf);
		return gf.createGeometryCollection(gs);
	}

	public static Geometry get(Geometry geom, Coordinate c, double coef, GeometryFactory gf) {
		if(geom instanceof Point) return get((Point)geom, c, coef, gf);
		else if(geom instanceof Polygon) return get((Polygon)geom, c, coef, gf);
		else if(geom instanceof LineString) return get((LineString)geom, c, coef, gf);
		else if(geom instanceof LinearRing) return get((LinearRing)geom, c, coef, gf);
		//logger.warning("Scaling of " + geom.getClass().getSimpleName() + " not supported yet.");
		return null;
	}
}
