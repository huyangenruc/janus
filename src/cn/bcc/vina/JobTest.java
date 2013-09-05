package cn.bcc.vina;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobStatus;

import cn.bcc.manage.CheckJob;
import cn.bcc.meta.HadoopConf;
import cn.bcc.util.HadoopFile;

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
		jobStatus = jobClient.getAllJobs();
		System.out.println(jobStatus.length);
		for(int i=0;i<jobStatus.length;i++){
		   if(jobStatus[i].getJobID().toString().equals("job_201308231723_0196")){
               System.out.println(jobStatus[i].getUsername());
                 System.out.println(jobStatus[i].getRunState());
                 System.out.println(jobStatus[i].isJobComplete());
                 System.out.println(jobStatus[i].getSchedulingInfo());
                 break;
             }
		}
		CheckJob check =new CheckJob();
		System.out.println(check.checkStatus("job_201308231723_0196"));	
		
		HadoopFile hf = new HadoopFile();
		//System.out.println(hf.localToHadoop("C:\\Users\\hu\\Desktop\\test\\2RH1C2.pdbqt", "/huyangen/2RH1C2.pdbqt"));
		//hf.localToHadoop("C:\\Users\\hu\\Desktop\\test\\2RH1C2.pdbqt", "/huyangen/vinaResult");
		//hf.HadoopToLocal("/huyangen/2RH1C2.pdbqt", "C:\\Users\\hu\\Desktop\\test");
		
		FileSystem fs = FileSystem.get((new HadoopConf()).getConf());
		FileStatus[] stats = fs.listStatus(new Path("/huyangen/vinaResult/JobID/20130829"));
		System.out.println(stats[0].getPath().getName());
		hf.getFromHadoop("/huyangen/vinaResult/JobID/20130829", "C:\\Users\\hu\\Desktop\\test\\test");
		String a = "/a/";
		a=a.substring(0, a.length()-1);
		System.out.println(a);
		boolean test = hf.exportFile("20130905", "result", "C:\\Users\\hu\\Desktop\\test\\");
		System.out.println(test);
	/*	
		Job job = new Job((new HadoopConf()).getConf(), "test");
		job.submit();
		String result =job.getJobID().toString()+","+job.getJobName();
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println(result);*/
		
	}

}
