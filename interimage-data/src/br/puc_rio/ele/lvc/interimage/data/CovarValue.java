package br.puc_rio.ele.lvc.interimage.data;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import net.imglib2.img.Img;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.ops.operation.iterable.unary.Mean;


/**
 * A UDF that returns the covariance bewteen two raster layers.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate CovarValue(raster#'0',raster#'1');
 * @author Patrick Happ
 *
 */

public class CovarValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @param input tuple; first column is assumed to have the raster layer
     * @exception java.io.IOException
     * @return covariance of the data, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() <2)
            return null;
        
		try {
			Object objImage = input.get(0);
			Object objImage2 = input.get(1);
			//TODO this should be generic and not only for DoubleType
			Img <DoubleType> ip = imageParser.parseData(objImage);
			Img <DoubleType> ip2 = imageParser.parseData(objImage2);
						
			DoubleType value =  new DoubleType();
			DoubleType value2 =  new DoubleType();
			
			Mean< DoubleType, DoubleType >  resMean = new Mean< DoubleType, DoubleType >();
			Mean< DoubleType, DoubleType >  resMean2 = new Mean< DoubleType, DoubleType >();
			
			double meanIdx = resMean.compute(ip.iterator(), value).get();
			double meanIdx2 = resMean2.compute(ip2.iterator(), value2).get();
			
			final Iterator< DoubleType > it = ip.iterator();
			final Iterator< DoubleType > it2 = ip2.iterator();
			double accSum=0;
			long i=0;

	 
	        // loop over the data and determine the covariance
			//covar(a,b)= 1/N * somatory[i=0...N](xa(i)-mean(xa))*(xb(i)-mean(xb))
	        while (it.hasNext() && it2.hasNext()){
	        	accSum=(it.next().get() - meanIdx)*(it2.next().get() - meanIdx2)+accSum;
	        	i++;
	        }
	                	
			return accSum/i;


		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}