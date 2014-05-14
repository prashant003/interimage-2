package br.puc_rio.ele.lvc.interimage.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * A UDF that returns the entropy of a raster layer.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate EntropyValue(raster#'0');
 * @author Patrick Happ
 *
 */
public class EntropyValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @exception java.io.IOException
     * @return entropy value of the layer, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() == 0)
            return null;
        
		try {			
			Object objImage = input.get(0);
			//TODO this should be generic and not only for Double
			List <Double> lst = imageParser.parseListData(objImage);
			
			final Map<Double, Integer> countMap = new HashMap<Double, Integer>();
			    
		    //iterate over data to find frequency
		    for (int n=0; n < lst.size(); n++) {
		        int count = 0;
		        
		        double val=lst.get(n);
		        if (countMap.containsKey(val)) {
		            count = countMap.get(val) + 1;
		        } else {
		            count = 1;
		        }			
		        countMap.put(val, count);
	    	}
		    
		    double accSum =0;
		    
		    //For each distinct value operate
		    for (final Map.Entry<Double, Integer> tuple : countMap.entrySet())  {
		    	double val =tuple.getValue();
		    	//TODO verify formula: different definitions in literature
		    	accSum= accSum + val * Math.log(val)/Math.log(2);
		    }
			 			
				
			return accSum*(-1);
		
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}