package cn.bcc.vina;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.bcc.meta.HadoopConf;



public class Test {
	private static void hint() {
        System.out.println("Continue to run a vina hadoop demo? [yes]/[no]");
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));
        try {
            String command = reader.readLine();
            
            
           if("yes".equals(command.trim())){
            	System.out.println("please input vina jobid,eg:20130909 [yes]/[no]");
                BufferedReader newReader = new BufferedReader(new InputStreamReader(
                        System.in));
                command = newReader.readLine();
                System.out.println(command.trim());
            } else  {
            	System.out.println("okay,exit now");
                System.exit(0);
            }
           
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
	
	
	public static void main(String args[]) {

		VinaHadoop job = new VinaHadoop();
		//final String jobPath = "hdfs://192.168.30.42:9000/vinaResult/vinaJobID/";     
		//final String srcDataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data"; 
		String vinaJobID = "20130910_test";
		ArrayList<String> test =new ArrayList<String>();
		//ArrayList<String> al =new ArrayList<String>();
		test.add("/pdbqt_1");
		test.add("/pdbqt_10");
		//test.add("/pdbqt_100");	
		//HadoopFile hf = new HadoopFile();
        //ArrayList<String> test = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
        int topK = 100;
        String confLocaPath = "C:/Users/hu/Desktop/filter/conf2";
        String receptorLocalPATH = "C:/Users/hu/Desktop/filter/2RH1C2.pdbqt";
        String seed = "1351189036";
		HashMap<String, String> result = job.startJob(confLocaPath,receptorLocalPATH,test,seed,topK,vinaJobID);
		System.out.println(result.get("flag"));
		System.out.println(result.get("hadoopID"));
		System.out.println(result.get("vinaJobID"));
		System.out.println(result.get("log"));
		//hint();

	}
}
	
	
