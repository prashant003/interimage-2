package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;

public abstract class gNode {

	private boolean running_=false;
	private boolean available_=true;
	private boolean executed_=false;
	private int requests_=0;
    static final AtomicLong NEXT_ID = new AtomicLong(0);
    final long id_ = NEXT_ID.getAndIncrement();    
 	
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

	public long getNodeId() {
		return id_;
	}

	public abstract JSONObject exportToJSON();
	
	public abstract Boolean importFromJSON(JSONObject path);
				
}
