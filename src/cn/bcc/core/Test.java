package cn.bcc.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import cn.bcc.util.HadoopFile;

public class Test {

	 private static void hint() {
	        System.out.println("Continue? [yes]/[no]");
	        BufferedReader reader = new BufferedReader(new InputStreamReader(
	                System.in));
	        try {
	            String command = reader.readLine();
	            if ("no".equals(command.trim())) {
	                System.exit(0);
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	            System.exit(-1);
	        }
	    }
	
	
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		/*String jobID = args[0];
        int bucket = Integer.parseInt(args[1]);
		final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";
        final String dataPath = "hdfs://192.168.30.42:9000/huyangen/vina/input";
        VinaJob job = new VinaJob();
		//String jobID = "20130826";
		//int bucket = 20;
		job.iniJob(jobPath,dataPath,jobID,bucket);
		final String input = jobPath+jobID+"/metadata";
		final String output = jobPath+jobID+"/output";
		job.startJob(input,output,jobID);*/
	/*	HadoopFile hf=new HadoopFile();
		
		String test1 = hf.readFromHadoop("hdfs://192.168.30.42:9000"+"/huyangen/vinaResult/JobID/20130829/out/ZINC58392841.pdbqt");
		String test2 = hf.readFromHadoop("/huyangen/vinaResult/JobID/20130829/out/ZINC58392841.pdbqt");
		//System.out.println(test1);
		test1.split("\n");
		Double a= Double.parseDouble(test1.split("\n")[1].split("    ")[1].trim());
		System.out.println(a);*/
		HadoopFile hf = new HadoopFile();
		/*ArrayList<String> al = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
		for(int i=0;i<al.size();i++){
			System.out.println(al.get(i));
		}*/
		System.out.println(hf.readFromHadoop("/huyangen/vinaResult/JobID/20130902/result/ZINC58393027.pdbqt"));
		//hf.HadoopToLocal("hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/20130902/result/ZINC58393027.pdbqt", "C:\\Users\\hu\\Desktop\\1.txt");
		
	}

}
