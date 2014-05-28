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

package br.puc_rio.ele.lvc.interimage.datamining;

import java.io.BufferedReader;
import java.lang.Exception;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 * A class that parses training data.<br><br> * 
 * This class makes some assumptions: 1) training data is given in CSV format using the "," symbol and 2) no header is passed in the file. 
 * 
 * @author Victor Quirita, Rodrigo Ferreira
 */
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
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Instances parseData(BufferedReader buff) {
		
		try {
			Instances dataInstance;
			//DataBag values = (DataBag)objData;
			
			int numAttributes = 0;	// N_Features + 1 Class
			
			List<String> inputClass = new ArrayList<String>();
			
			List<String[]> dataset = new ArrayList<String[]>();
			
			// To find the number of samples (instances in a bag)
			String line;			
	        while ((line = buff.readLine()) != null) {
	        	if (!line.isEmpty()) {
	        		String[] data = line.split(",");
	        		if (numAttributes==0)
	        			numAttributes = data.length;
	        		inputClass.add(data[data.length-1]);
	        		dataset.add(data);
	        	}
	        }
			
			HashSet classSet = new HashSet(inputClass);

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
			int index = 0;
			for (int k=0; k<dataset.size(); k++)
			{
				
				for (int i = 0; i < numAttributes-1; i++)
					tmpData.setValue((weka.core.Attribute) atts.elementAt(i), DataType.toDouble(dataset.get(k)[i]));
				//tmpData.setValue((weka.core.Attribute) atts.elementAt(numAttributes-1), DataType.toString(tuple.get(numAttributes-1)));
				dataInstance.add(tmpData);
				dataInstance.instance(index).setValue(numAttributes-1, DataType.toString(dataset.get(k)[numAttributes-1]));
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