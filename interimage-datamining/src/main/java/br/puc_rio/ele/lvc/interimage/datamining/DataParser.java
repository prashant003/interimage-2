package br.puc_rio.ele.lvc.interimage.datamining;

import java.lang.Exception;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

@SuppressWarnings("deprecation")
public class DataParser {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Instances parseData(Object objData) {
		
		try {
			Instances dataInstance;
			DataBag values = (DataBag)objData;
			int numAttributes = values.iterator().next().size();		// N_Features + 1 Class
			int bagSize = 0;											// To set the number of train samples
			
			// To find the number of samples (instances in a bag)
			for (Iterator<Tuple> it = values.iterator(); it.hasNext();)
			{
				it.next();
				bagSize = bagSize + 1;
			}
			
			// Code for find the different classes names in the input 
			String[] inputClass = new String[bagSize];					// String vector with the samples class's names
			int index = 0;
			for (Iterator<Tuple> it = values.iterator(); it.hasNext();)
			{
				Tuple tuple = it.next();
				inputClass[index] = DataType.toString(tuple.get(numAttributes-1));
				index = index + 1;
			}

			HashSet classSet = new HashSet(Arrays.asList(inputClass));

			String[] classValue = (String[])classSet.toArray(new String[0]);
			// To set the classes names in the attribute for the instance

			FastVector classNames = new FastVector();
			for (int i=0; i < classValue.length; i++)
				classNames.addElement(classValue[i]);
			
			// Creating the instance model N_Features + 1_ClassNames

			FastVector atts = new FastVector();
			for (int i=0; i < numAttributes-1; i++)
				atts.addElement(new Attribute("att" + i));
			dataInstance = new Instances("MyRelation", atts, numAttributes);
			dataInstance.insertAttributeAt(new Attribute("ClassNames", classNames),numAttributes-1);
			
			// To set the instance values for the dataInstance model created 
			Instance tmpData = new DenseInstance(numAttributes);
			index = 0;
			for (Iterator<Tuple> it = values.iterator(); it.hasNext();)
			{
				Tuple tuple = it.next();
				for (int i = 0; i < numAttributes-1; i++)
					tmpData.setValue((weka.core.Attribute) atts.elementAt(i), DataType.toDouble(tuple.get(i)));
				//tmpData.setValue((weka.core.Attribute) atts.elementAt(numAttributes-1), DataType.toString(tuple.get(numAttributes-1)));
				dataInstance.add(tmpData);
				dataInstance.instance(index).setValue(numAttributes-1, DataType.toString(tuple.get(numAttributes-1)));
				index = index + 1;
			}
			
			// Setting the class index
			dataInstance.setClassIndex(dataInstance.numAttributes() - 1);
			
			return dataInstance;
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}
}