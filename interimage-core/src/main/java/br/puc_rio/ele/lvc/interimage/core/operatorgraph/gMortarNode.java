package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import com.mortardata.api.v2.JobRequest;
import com.mortardata.api.v2.Jobs;
import com.mortardata.api.v2.Jobs.JobStatus;

public class gMortarNode extends gNode {
	private String pigScriptPath;
	private Jobs jobs_;
	private String projectName;
	private String codeVersion;
	private int clusterSize;
	
	public gMortarNode(String pigSPath){
		pigScriptPath=pigSPath;
	}

	public String getProjectName() {
		return projectName;
	}
	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}
	
	public int getClusterSize() {
		return clusterSize;
	}
	public void setClusterSize(int clusterSize) {
		this.clusterSize = clusterSize;
	}
	
	public String getCodeVersion() {
		return codeVersion;
	}
	public void setCodeVersion(String codeVersion) {
		this.codeVersion = codeVersion;
	}
	
	public String getPigScriptPath() {
		return pigScriptPath;
	}
	public void setPigScriptPath(String pigScriptPath) {
		this.pigScriptPath = pigScriptPath;
	}
	
	protected int execute()
	{
		JobRequest jobRequest = new JobRequest(projectName, pigScriptPath, codeVersion, clusterSize);
		String jobId;
		JobStatus finalJobStatus=JobStatus.UNKNOWN;
		try {
			jobId = jobs_.postJob(jobRequest);
			finalJobStatus = jobs_.blockUntilJobComplete(jobId);
			if (finalJobStatus==JobStatus.SUCCESS)
					return 1;
			else
					return 0;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public Jobs getJobs_() {
		return jobs_;
	}
	public void setJobs_(Jobs jobs_) {
		this.jobs_ = jobs_;
	}
}
