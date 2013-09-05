package cn.bcc.vina;

import java.io.IOException;
import java.util.ArrayList;

import cn.bcc.util.HadoopFile;

public class Test {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{

		VinaHadoop job = new VinaHadoop();
		final String jobPath = "hdfs://192.168.30.42:9000/vinaResult/vinaJobID/";     
		final String srcDataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data"; 
		String vinaJobID = "20130905_24map6000file";
		/*ArrayList<String> test =new ArrayList<String>();
		test.add("/pdbqt_1");
		test.add("/pdbqt_10");
		//test.add("/pdbqt_100");	
*/       
		HadoopFile hf = new HadoopFile();
        ArrayList<String> test = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
        int bucket = Integer.parseInt("24");
        int topK = 200;
        String confHDFSPath = "/huyangen/vina/conf2";
        String receptorHDFSPATH = "/huyangen/vina/2RH1C2.pdbqt";
        String seed = "1351189036";
		String result = job.startJob(confHDFSPath,receptorHDFSPATH,test,seed,topK,vinaJobID,bucket);
		System.out.println(result);
	}
}
	
	
