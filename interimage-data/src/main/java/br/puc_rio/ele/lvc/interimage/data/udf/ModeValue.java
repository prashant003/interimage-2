package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap; 
import java.util.Map; 

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.data.DataParser;


/**
 * A UDF that returns the mode of a raster layer.
 * Example:
 * 		A = load 'mydata' as (raster);
 * 		B = foreach A generate ModeValue(raster#'0');
 * @author Patrick Happ
 *
 */
public class ModeValue extends EvalFunc<Double> {
	
	private final DataParser imageParser = new DataParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the raster layer
     * @exception java.io.IOException
     * @return the mode of the layer, or null in case of processing error
     */
	@Override
	public Double exec(Tuple input) throws IOException {		
		if (input == null || input.size() == 0)
            return null;
        
		try {			
			Object objImage = input.get(0);
			//TODO this should be generic and not only for Double
			List <Double> lst = imageParser.parseListData(objImage);
			
			final List<Double> modes = new ArrayList<Double>();
			final Map<Double, Integer> countMap = new HashMap<Double, Integer>();
			    int max = -1;
			    
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
			        
		        	//Get the mode
			        //keep track of the whole list (multimodal)
			        //Is it better than read the list again?
			        if (count > max) {
			        	max = count;
			        	modes.clear();
			        	modes.add(val);
			        }
			        else if(count == max)
			        	modes.add(val);
			    }
			    
			 				
			//TODO Return multiple results when it exists
			if (max<2)
				return null;
			else
				return modes.get(0);
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}