package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import org.json.JSONObject;

import com.mortardata.api.v2.JobRequest;
import com.mortardata.api.v2.Jobs;
import com.mortardata.api.v2.Jobs.JobStatus;

public class gMortarOperator extends gOperator {
	private String pigScriptPath_;
	private Jobs jobs_;
	private String projectName_;
	private String codeVersion_;
	private int clusterSize_;

	public gMortarOperator(){
	}
	
	public gMortarOperator(String pigSPath){
		pigScriptPath_=pigSPath;
	}

	public String getProjectName() {
		return projectName_;
	}
	public void setProjectName(String projectName) {
		this.projectName_ = projectName;
	}
	
	public int getClusterSize() {
		return clusterSize_;
	}
	public void setClusterSize(int clusterSize) {
		this.clusterSize_ = clusterSize;
	}
	
	public String getCodeVersion() {
		return codeVersion_;
	}
	public void setCodeVersion(String codeVersion) {
		this.codeVersion_ = codeVersion;
	}
	
	public String getPigScriptPath() {
		return pigScriptPath_;
	}
	public void setPigScriptPath(String pigScriptPath) {
		this.pigScriptPath_ = pigScriptPath;
	}
	
	protected int execute()
	{
		JobRequest jobRequest = new JobRequest(projectName_, pigScriptPath_, codeVersion_, clusterSize_);
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

	@Override
	public JSONObject exportToJSON() {
		JSONObject my_obj = new JSONObject();
		
		my_obj.put("type", "gMortarOperator");
		
		my_obj.put("pigScriptPath", pigScriptPath_);
		my_obj.put("projectName", projectName_);
		my_obj.put("codeVersion", codeVersion_);
		my_obj.put("clusterSize", clusterSize_);

		return my_obj;
	}

	@Override
	public Boolean importFromJSON(JSONObject obj){
		if (obj.getString("pigScriptPath")=="")
			return false;
		else {
			this.setPigScriptPath(obj.getString("pigScriptPath"));
		}
		if (obj.getString("projectName")=="")
			return false;
		else {
			this.setProjectName(obj.getString("projectName"));
		}
		if (obj.getString("codeVersion")=="")
			return false;
		else {
			this.setCodeVersion(obj.getString("codeVersion"));
		}
		if (obj.getString("clusterSize")=="")
			return false;
		else {
			this.setClusterSize(obj.getInt("clusterSize"));
		}
		return true;	
	}
}
