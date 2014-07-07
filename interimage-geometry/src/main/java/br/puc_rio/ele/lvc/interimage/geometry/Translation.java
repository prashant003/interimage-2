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
public class Translation {
	//private static Logger logger = Logger.getLogger(Translation.class.getName());


	public static Coordinate get(Coordinate coord, double dx, double dy){
		return new Coordinate(coord.x+dx, coord.y+dy);
	}

	public static Coordinate[] get(Coordinate[] coords, double dx, double dy){
		Coordinate[] coord_= new Coordinate[coords.length];
		for(int i=0; i<coords.length; i++) coord_[i] = get(coords[i], dx, dy);
		return coord_;
	}


	public static Point get(Point geom, double dx, double dy, GeometryFactory gf){
		return gf.createPoint( get(geom.getCoordinate(), dx, dy) );
	}


	public static LineString get(LineString geom, double dx, double dy, GeometryFactory gf){
		return gf.createLineString(get(geom.getCoordinates(), dx, dy));
	}

	public static LinearRing get(LinearRing geom, double dx, double dy, GeometryFactory gf){
		return gf.createLinearRing(get(geom.getCoordinates(), dx, dy));
	}


	public static Polygon get(Polygon geom, double dx, double dy, GeometryFactory gf){
		LinearRing lr = get((LinearRing)geom.getExteriorRing(), dx, dy, gf);
		LinearRing[] lr_ = new LinearRing[geom.getNumInteriorRing()];
		for(int j=0; j<geom.getNumInteriorRing(); j++) lr_[j] = get((LinearRing)geom.getInteriorRingN(j), dx, dy, gf);
		return gf.createPolygon(lr, lr_);
	}


	public static GeometryCollection get(GeometryCollection geomCol, double dx, double dy, GeometryFactory gf){
		Geometry[] gs = new Geometry[geomCol.getNumGeometries()];
		for(int i=0; i< geomCol.getNumGeometries(); i++) gs[i] = get(geomCol.getGeometryN(i), dx, dy, gf);
		return gf.createGeometryCollection(gs);
	}

	public static Geometry get(Geometry geom, double dx, double dy, GeometryFactory gf){
		if(geom instanceof Point) return get((Point)geom, dx, dy, gf);
		else if(geom instanceof Polygon) return get((Polygon)geom, dx, dy, gf);
		else if(geom instanceof LineString) return get((LineString)geom, dx, dy, gf);
		else if(geom instanceof LinearRing) return get((LinearRing)geom, dx, dy, gf);
		//logger.warning("Translation of " + geom.getClass().getSimpleName() + " not supported yet.");
		return null;
	}
}
