package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

public abstract class gNode {

	private boolean running=false;
	private boolean available=true;
	private boolean executed=false;
	private int requests=0;
	
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
		return running;
	}

	private void setRunning(boolean running) {
		this.running = running;
	}

	public boolean isAvailable() {
		return available;
	}

	public void setAvailable(boolean available) {
		this.available = available;
	}

	public boolean isExecuted() {
		return executed;
	}

	private void setExecuted(boolean executed) {
		this.executed = executed;
	}

	public int getRequests() {
		return requests;
	}

	public void improveRequest() {
		requests = requests +1 ;
	}
	
	public void setRequests(int requests) {
		this.requests = requests;
	}
				
}
