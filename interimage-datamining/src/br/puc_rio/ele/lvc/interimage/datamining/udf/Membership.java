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

package br.puc_rio.ele.lvc.interimage.datamining.udf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.datamining.FuzzySet;

/**
 * A UDF that returns the membership value of a membership function (fuzzy set) positioned at the given attribute value.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (attrib);<br>
 * 		B = foreach A generate Membership('fuzzy set',attrib);<br>
 * @author Rodrigo Ferreira
 *
 */
public class Membership extends EvalFunc<Double> {

	String _fuzzySetsUrl = null;
	Map<String,List<Map<String,Double>>> _fuzzySets = null;
	
	/**Constructor that takes the fuzzy sets URL.*/
	public Membership(String fuzzySetsUrl) {
		_fuzzySetsUrl = fuzzySetsUrl;
	}
	
	/**This method computes the membership value of a membership function positioned at the given attribute value.*/
	private Double computeMembershipValue(List<Map<String,Double>> points, Double value) {
		
		Double returnValue = null;
		
		try {
		
			if (points.size() > 0) {
			
				if (value <= points.get(0).get("first")) {
					returnValue = points.get(0).get("second");
				} else if (value >= points.get(points.size()-1).get("first")) {
					returnValue = points.get(points.size()-1).get("second");
				} else {
					
					for (int i=0; i<(points.size()-1); i++) {
						
						if ((value>=points.get(i).get("first")) && (value<=points.get(i+1).get("first"))) {
							double a = (points.get(i+1).get("second")-points.get(i).get("second")) / (points.get(i+1).get("first")-points.get(i).get("first"));
							double b = points.get(i).get("second") - (a*points.get(i).get("first"));
							returnValue = (a*value) + b;
						}
						
					}
					
				}
				
			}
				
		} catch (Exception e) {
			System.err.println("Failed to compute the membership value; error - " + e.getMessage());
		}
		
		return returnValue;
			
	}
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have the fuzzy set name<br>
     * second column is assumed to have the attribute value
     * @exception java.io.IOException
     * @return membership value
     */
	@SuppressWarnings("unchecked")
	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
            return null;
        
		if (_fuzzySets == null) {
			
			_fuzzySets = new HashMap<String,List<Map<String,Double>>>();
			
			try {
			
				if (!_fuzzySetsUrl.isEmpty()) {
	    	        
	        		URL url  = new URL(_fuzzySetsUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
	                InputStream buff = new BufferedInputStream(urlConn.getInputStream());
	    	        ObjectInputStream in = new ObjectInputStream(buff);
	    			
	    		    List<FuzzySet> fuzzySets = (List<FuzzySet>)in.readObject();
	    		    
	    		    in.close();
				    
				    for (FuzzySet fs : fuzzySets) {
			        	_fuzzySets.put(fs.getName(), fs.getPoints());
				    }
	    	        
	        	}
				
			} catch (Exception e) {
				throw new IOException("Caught exception reading fuzzy sets file ", e);
			}
			
		}
		
		try {
			
			String fuzzySetName = DataType.toString(input.get(0));
			Double value = DataType.toDouble(input.get(1));			
			
			return computeMembershipValue(_fuzzySets.get(fuzzySetName),value);
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}
