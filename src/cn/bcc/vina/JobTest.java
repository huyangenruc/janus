package cn.bcc.vina;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapred.JobStatus;

import cn.bcc.meta.HadoopConf;

public class JobTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		JobClient jobClient = null;
		JobStatus[] jobStatus =null ;
		InetSocketAddress address = new InetSocketAddress("192.168.30.41",9001);
		jobClient = new JobClient(address,(new HadoopConf()).getConf());
		//jobClient.setConf((new HadoopConf()).getConf());
		
		jobStatus = jobClient.getAllJobs();
		JobID jobID = null;
		String jobName = "vinaHadoop20130903test";
		System.out.println(jobStatus.length);
		
		
		for(int i=0;i<jobStatus.length;i++){
			//System.out.println(jobStatus[i].getRunState());
			 RunningJob rj = jobClient.getJob(jobStatus[i].getJobID());
			
			 System.out.println(jobStatus[i].getJobID().toString()+" "+jobStatus[i].getUsername());
			 //jobStatus[i].getJobRunState();
			/* if (rj.getJobName().trim().equals(jobName)) {
                 jobID = jobStatus[i].getJobID();
                 break;
             }*/
		}
		
	
	}

}
