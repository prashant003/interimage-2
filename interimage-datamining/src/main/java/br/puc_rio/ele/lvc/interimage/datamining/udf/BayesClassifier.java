package br.puc_rio.ele.lvc.interimage.datamining.udf;

import java.io.IOException;
import java.lang.Exception;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.datamining.DataParser;
import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;

public class BayesClassifier extends EvalFunc<String> {

	private final DataParser dataParser = new DataParser();
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null)
            return null;
        
		try {
			Integer numFeatures = DataType.toInteger(input.get(0));
			double [] testData;
			testData = new double[numFeatures];
			for (int i=0; i<numFeatures; i++)
				testData[i] = DataType.toDouble(input.get(i+1));
			
			Object objData = input.get(input.size()-1);
			Instances trainData = dataParser.parseData(objData);
			Classifier csfr = null;
			csfr = (Classifier)Class.forName("weka.classifiers.bayes.NaiveBayes").newInstance();
			csfr.buildClassifier(trainData);
			double classification = 0;
			
			Instance myinstance = trainData.instance(0);
			for (int i=0; i<numFeatures; i++)
				myinstance.setValue(i, testData[i]);
			classification = csfr.classifyInstance(myinstance);

			return myinstance.attribute(trainData.classIndex()).value((int) classification);
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}