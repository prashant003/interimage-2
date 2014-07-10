package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

public abstract class gNode {

	private boolean running_=false;
	private boolean available_=true;
	private boolean executed_=false;
	private int requests_=0;
	
	public void run(){
		this.setRunning(true);
		//exec something	
		execute();
		this.setRunning(false);
		this.setExecuted(true);
		this.setAvailable(false);
	}

	protected abstract int execute();
	
	public boolean isRunning() {
		return running_;
	}

	private void setRunning(boolean running) {
		this.running_ = running;
	}

	public boolean isAvailable() {
		return available_;
	}

	public void setAvailable(boolean available) {
		this.available_ = available;
	}

	public boolean isExecuted() {
		return executed_;
	}

	private void setExecuted(boolean executed) {
		this.executed_ = executed;
	}

	public int getRequests() {
		return requests_;
	}

	public void improveRequest() {
		requests_ = requests_ +1 ;
	}
	
	public void setRequests(int requests) {
		this.requests_ = requests;
	}
				
}
