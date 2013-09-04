package cn.bcc.vina;

import java.io.IOException;
import java.util.ArrayList;

public class Test {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{

		VinaHadoop job = new VinaHadoop();
		final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";     
		final String dataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data"; 
		String jobID = "huyangen";
		ArrayList<String> test =new ArrayList<String>();
		test.add("/pdbqt_1");
		test.add("/pdbqt_10");
		//test.add("/pdbqt_100");	
        //HadoopFile hf = new HadoopFile();
        //ArrayList<String> test = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
        int bucket = Integer.parseInt("12");
        int topK = 100;
		String result = job.startJob(jobPath, dataPath, jobID, test, bucket, topK);
		System.out.println(result);
	}
}
	
	
