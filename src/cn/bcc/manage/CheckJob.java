package cn.bcc.manage;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapred.JobStatus;

import cn.bcc.meta.HadoopConf;

/**
 * check hadoop job status
 * @author hu
 *
 */
public class CheckJob {
	
	JobClient jobClient = null;
	JobStatus[] jobStatus =null ;
	public CheckJob() throws IOException{
		InetSocketAddress address = new InetSocketAddress("192.168.30.41",9001);
		jobClient = new JobClient(address,(new HadoopConf()).getConf());
		jobStatus = jobClient.getAllJobs();
	}
	
	/**
	 * check job status through hadoop job id
	 * @param hadoopJobID 
	 * @return
	 */
	public int checkStatus(String hadoopJobID){
		int status_int = 0 ;
		for(int i=0;i<jobStatus.length;i++){
			if(jobStatus[i].getJobID().toString().equals(hadoopJobID)){
				status_int = jobStatus[i].getRunState();
				break;
			}
		}
		return status_int;
	}
	
	
	private JobID getJobIDByJobName(String jobName){
		JobID jobID = null;
		try {
			for(int i=0;i<jobStatus.length;i++){
				 RunningJob rj = jobClient.getJob(jobStatus[i].getJobID());
				 //jobStatus[i].getJobRunState();
				 if (rj.getJobName().trim().equals(jobName)) {
	                  jobID = jobStatus[i].getJobID();
	                  break;
	              }
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobID;
	}
	
	
	private  String getStatusByJobID(JobClient jobClient, JobStatus[] jobStatus, JobID jobID) throws IOException {
	       int status_int = 0;
	       jobStatus = jobClient.getAllJobs();
	       for (int i = 0; i < jobStatus.length; i++) {
	           if (jobStatus[i].getJobID().getId() == jobID.getId()) {
	              status_int = jobStatus[i].getRunState();
	              
	              break;
	           }
	       }
	       String status = "";
	       switch (status_int) {
	       case 1:
	           status = "RUNNING";
	           break;
	       case 2:
	           status = "SUCCEEDED";
	           break;
	       case 3:
	           status = "FAILED";
	           break;
	       case 4:
	           status = "PREP";
	           break;
	       case 5:
	           status = "KILLED";
	           break;
	       default:
	           break;
	       }
	       return status;

	    }

}
