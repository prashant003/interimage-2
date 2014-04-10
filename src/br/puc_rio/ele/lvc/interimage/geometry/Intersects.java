package br.puc_rio.ele.lvc.interimage.geometry;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that tests whether a geometry intersects another geometry.
 * Example:
 * 		A = load 'mydata1' as (geom1);
 * 		B = load 'mydata2' as (geom2);
 * 		C = cross A, B
 * 		D = filter C by Intersects(geom1,geom2);
 * @author Rodrigo Ferreira
 *
 */
public class Intersects extends EvalFunc<Boolean> {
	
	private final GeometryParser geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple; first column is assumed to have a geometry; second column is assumed to have a geometry
     * @exception java.io.IOException
     * @return boolean value
     */
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
            return null;
        
		try {			
			Object objGeometry1 = input.get(0);
			Object objGeometry2 = input.get(1);
			Geometry geometry1 = geometryParser.parseGeometry(objGeometry1);
			Geometry geometry2 = geometryParser.parseGeometry(objGeometry2);
			return geometry1.intersects(geometry2);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
