package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.data.DataParser;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.ops.operation.iterable.unary.Mean;

/**
 * A UDF that returns the brightness of raster layers.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate BrightnessValue(raster);
 * @author Patrick Happ
 * @author Rodrigo Ferreira
 *
 */
@SuppressWarnings("unchecked")
public class BrightnessValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; assuming the raster with all layers
     * @exception java.io.IOException
     * @return brightness of the data, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() == 0)
            return null;
        
		try {
			Map<String, Object> objImages = (Map<String, Object>)input.get(0);
			double sum = 0;			
			
			for (Map.Entry<String, Object> entry : objImages.entrySet()) {
			//TODO this should be generic and not only for DoubleType
				Img <DoubleType> ip = imageParser.parseData(entry.getValue());
				DoubleType value =  new DoubleType();;				
				Mean< DoubleType, DoubleType >  res = new Mean< DoubleType, DoubleType >();
				sum = sum + res.compute(ip.iterator(), value).get();				
			}
						
			return sum/(double)objImages.size(); 

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}