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
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.TileManager;

/**
 * A UDF that computes the tiles a geometry intersects.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom, data, props);<br>
 * 		B = foreach A generate geom, data, ToProps(Tile(geom), 'tile', props) as props;
 * @author Rodrigo Ferreira
 *
 */
public class Tile extends EvalFunc<String> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	private TileManager _tileManager = null;
	private Integer _tileSize = null;
	private Double _resolution = null;
	
	/**Constructor that takes tile size and resolution in meters.*/
	public Tile(String tileSize, String resolution) {
		if (!tileSize.isEmpty())
			_tileSize = Integer.parseInt(tileSize);
		else
			_tileSize = 256;
		
		if (!resolution.isEmpty())
			_resolution = Double.parseDouble(resolution);
		else
			_resolution = 1.0;
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry
     * @exception java.io.IOException
     * @return string with intersecting tiles
     */
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
     	
		if (_tileManager == null) {
			_tileManager = new TileManager(_tileSize * _resolution);
		}
		
		try {
			Object objGeometry = input.get(0);
			Geometry geom = _geometryParser.parseGeometry(objGeometry);
			
			double[] bbox = new double[] {geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxX(), geom.getEnvelopeInternal().getMaxY()};
			
			List<String> tiles = _tileManager.getTiles(bbox);
			
			String tileString = new String();
			
			boolean first = true;
			for (String i : tiles) {
				if (first) {
					tileString = i;
					first = false;
				} else {
					tileString = tileString + "," + i;
				}
			}
			
			return tileString;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
	
}
