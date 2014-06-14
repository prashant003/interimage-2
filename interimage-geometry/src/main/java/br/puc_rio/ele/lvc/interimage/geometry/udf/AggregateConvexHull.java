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

package br.puc_rio.ele.lvc.interimage.geometry.udf;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF that computes the aggregate convex hull of a set of geometries. It works efficiently by computing partial convex hulls and then the overall convex hull.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom, data, props);<br>
 * 		B = group A by props#'class';<br>
 * 		C = foreach B generate group, AggregateConvexHull(A);
 * @author Rodrigo Ferreira
 *
 */
public class AggregateConvexHull extends EvalFunc<DataByteArray> implements Algebraic, Accumulator<DataByteArray> {

	private static final GeometryParser _geometryParser = new GeometryParser();
	
	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		return new DataByteArray(new WKBWriter().write(convexHull(input)));
	}
	
	public String getInitial() { return Initial.class.getName(); }
	
	public String getIntermed() { return Intermed.class.getName(); }
	
	public String getFinal() { return Final.class.getName(); }
	
	//Initial
	static public class Initial extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			//Retrieve the first element (tuple) in the given bag
			return ((DataBag)input.get(0)).iterator().next();
		}
	}

	//Intermed
	static public class Intermed extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(new WKBWriter().write(convexHull(input)));
		}
	}
	
	//Final
	static public class Final extends EvalFunc<DataByteArray> {
		@Override
		public DataByteArray exec(Tuple input) throws IOException {
			return new DataByteArray(new WKBWriter().write(convexHull(input)));
		}
	}
	
	static protected Geometry convexHull(Tuple input) throws ExecException {
		DataBag values = (DataBag)input.get(0);
		
		if (values.size() == 0)
			return null;
	    
		Geometry[] all_geoms = new Geometry[(int)values.size()];
	    
		int idx = 0;
		for (Tuple one_geom : values) {
			Geometry parsedGeom = _geometryParser.parseGeometry(one_geom.get(0));
			all_geoms[0] = parsedGeom;
			idx = idx + 1;
	    }
	    
		//Do a union of all_geometries in the recommended way (using buffer(0))
	    GeometryCollection geom_collection = new GeometryCollection(all_geoms, new GeometryFactory());
	    return geom_collection.convexHull();
	}
	
	Geometry _partialConvexHull;	
	  
	public void accumulate(Tuple b) throws IOException {
			    
		DataBag bag = (DataBag) b.get(0);
	    
		Geometry[] all_geoms;
		
		int idx = 0;
		if (_partialConvexHull != null) {
			all_geoms = new Geometry[(int)bag.size()+1];
			all_geoms[idx] = _partialConvexHull;
			idx = idx + 1;
		} else {
			all_geoms = new Geometry[(int)bag.size()];
		}
	    
		for (Tuple t : bag) {
			Geometry geom = _geometryParser.parseGeometry(t.get(0));
			all_geoms[idx] = geom;
			idx = idx + 1;
	    }
	    
		_partialConvexHull = new GeometryCollection(all_geoms, new GeometryFactory()).convexHull();
	}

	public DataByteArray getValue() {
		return new DataByteArray(new WKBWriter().write(_partialConvexHull));
	}

	public void cleanup() {
		_partialConvexHull = null;
	}
	
}
