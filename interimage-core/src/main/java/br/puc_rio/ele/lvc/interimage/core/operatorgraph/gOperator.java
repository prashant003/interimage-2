package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.util.ArrayList;
import java.util.List;

public abstract class gOperator extends gNode {
	private List<String> inputClasses_ = new ArrayList<String>();
	private List<String> outputClasses_ = new ArrayList<String>();
	
	public List<String> getInputClasses() {
		return inputClasses_;
	}
	public void setInputClasses(List<String> inputClasses_) {
		this.inputClasses_ = inputClasses_;
	}
	public List<String> getOutputClasses() {
		return outputClasses_;
	}
	public void setOutputClasses(List<String> outputClasses_) {
		this.outputClasses_ = outputClasses_;
	}
	
	public void addInputClass(String classe) {
		inputClasses_.add(classe);
	}
	public void addOutputClass(String classe) {
		outputClasses_.add(classe);
	}
	
	public boolean hasInputClass(String classe) {
		return inputClasses_.contains(classe);
	}
	public boolean hasOutputClass(String classe) {
		return outputClasses_.contains(classe);
	}
}
