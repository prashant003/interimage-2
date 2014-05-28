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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Exception;
import java.net.URL;
import java.net.URLConnection;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.datamining.DataParser;
import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;

/**
 * A UDF that classifies the tuples using a Bayes classifier.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom, data, properties);<br>
 * 		B = foreach A generate *, BayesClassifier(properties#'attr1', properties#'attr2', properties#'attr3') as class;
 * @author Victor Quirita, Rodrigo Ferreira
 */
public class BayesClassifier extends EvalFunc<String> {

	private final DataParser _dataParser = new DataParser();
	
	private String _trainUrl;
	private Instances _trainData = null;
	
	/**Constructor that takes the training data URL.*/
	public BayesClassifier(String trainUrl) {
		_trainUrl = trainUrl;
	}
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null)
            return null;
        
		if (_trainData == null) {
		
			//Reads train data
	        try {
	        	
	        	if (!_trainUrl.isEmpty()) {
	        		
	        		URL url  = new URL(_trainUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
	                InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
			        BufferedReader buff = new BufferedReader(inStream);
			        
			        _trainData = _dataParser.parseData(buff);
	
	        	}
	        } catch (Exception e) {
				throw new IOException("Caught exception reading training data file ", e);
			}
	        
		}
		
		try {
			Integer numFeatures = input.size();
			
			double [] testData;			
			testData = new double[numFeatures];
			
			for (int i=0; i<numFeatures; i++)
				testData[i] = DataType.toDouble(input.get(i));
			
			Classifier csfr = null;
			csfr = (Classifier)Class.forName("weka.classifiers.bayes.NaiveBayes").newInstance();
			csfr.buildClassifier(_trainData);
			double classification = 0;
			
			Instance myinstance = _trainData.instance(0);
			for (int i=0; i<numFeatures; i++)
				myinstance.setValue(i, testData[i]);
			classification = csfr.classifyInstance(myinstance);

			return myinstance.attribute(_trainData.classIndex()).value((int) classification);
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}