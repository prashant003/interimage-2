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
import net.imglib2.ops.operation.iterable.unary.Sum;

/**
 * A UDF that returns the ratio (contribution of a layer in brightness) of a raster layer.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate RatioValue(raster,'0');
 * @author Patrick Happ
 *
 */
@SuppressWarnings("unchecked")
public class RatioValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; assuming the raster with all layers; and the index of requested layer
     * @exception java.io.IOException
     * @return ratio of the layer, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() < 2)
            return null;
        
		try {
			Map<String, Object> objImages = (Map<String, Object>)input.get(0);
			
			String strIndex = (String)input.get(1);;
			int idx = Integer.parseInt(strIndex);
			
			double accSum = 0;
			double idxSum=0;
			int band=0;
			
			for (Map.Entry<String, Object> entry : objImages.entrySet()) {
			//TODO this should be generic and not only for DoubleType
				Img <DoubleType> ip = imageParser.parseData(entry.getValue());
				DoubleType value =  new DoubleType();				
				Sum< DoubleType, DoubleType >  res = new Sum< DoubleType, DoubleType >();
				
				double sum= res.compute(ip.iterator(), value).get();
				accSum = accSum + sum;
				if (band==idx)
					idxSum=sum;
				
				band++;
			}
						
			return idxSum/accSum; 

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}