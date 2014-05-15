package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.data.DataParser;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.real.DoubleType;

/**
 * A UDF that returns the correlation bewteen two raster layers.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate CorrelValue(raster#'0',raster#'1');
 * @author Patrick Happ
 *
 */

public class CorrelValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @param input tuple; first column is assumed to have the raster layer
     * @exception java.io.IOException
     * @return correlation of the data, or null in case of processing error
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
			
		
			final Iterator< DoubleType > it = ip.iterator();
			final Iterator< DoubleType > it2 = ip2.iterator();
			double sumx =0;
			double sumy =0;
			double sumx2 =0;
			double sumy2 =0;
			double sumxy=0;

			long i=0;
	 
	        // loop over the data to accumulate the values
	        while (it.hasNext() && it2.hasNext()){
	        	//accSum=( - meanIdx)*(it2.next().get() - meanIdx2)+accSum;
	        	double xi=it.next().get();
	        	double yi=it2.next().get();
	        	
	        	sumx=sumx+ xi;
	        	sumy=sumy+ yi;
	        	sumx2=sumx2+ xi*xi;
	        	sumy2=sumy2+ yi*yi;
	        	sumxy=sumxy+ xi*yi;
	        	
	        	i++;
	        }
	        
	        //Apply the formula: [n*sum(xiyi) - sum(xi)*sum(yi)] / [sqrt((n*sum(xi^2) - (sum(xi)^2)) * sqrt((n*sum(yi^2) - (sum(yi)^2)) ]
	        double correl= (i*sumxy - sumx*sumy)/Math.sqrt((i*sumx2 - sumx*sumx) * (i*sumy2 - sumy*sumy));
	        
	        return correl;	
			
	        
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}