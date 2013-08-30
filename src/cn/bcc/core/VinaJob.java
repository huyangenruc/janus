package cn.bcc.core;

import cn.bcc.core.VinaHadoop.VinaMapper;
import cn.bcc.core.VinaHadoop.VinaReducer;
import cn.bcc.meta.*;
import cn.bcc.util.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VinaJob {
	
	 public static class VinaMapper extends Mapper<Object, Text, Text, Text>{
		    private Text word = new Text();
		    public void setup(Context context) throws IOException, InterruptedException{
		    	Configuration conf = context.getConfiguration();
		    	String conf2 = conf.get("conf2");
		    	String receptor = conf.get("receptor");
		    	HadoopFile hf = new HadoopFile();
		    	hf.HadoopToLocal(conf2, "/home/hadoop/vinamap/"+conf2.substring(conf2.lastIndexOf("/")+1));
		    	hf.HadoopToLocal(receptor, "/home/hadoop/vinamap/"+receptor.substring(receptor.lastIndexOf("/")+1));
		    }
		    public void map(Object key, Text value, Context context

		                    ) throws IOException, InterruptedException {
		    	String path = value.toString();
		    	Configuration conf = context.getConfiguration();
		    	String jobID = conf.get("jobID");
		    	
		    	HadoopFile hf=new HadoopFile();
		    	//VinaJni vinaInstance = new VinaJni();
		    	//String vinaResult=vinaInstance.getVinaResult("", " ", "",10);
		    	//String test = "hdfs://192.168.30.42:9000/huyangen/vina/input/3/c69";
		    	InetAddress addr = InetAddress.getLocalHost();
		    	String ip=addr.getHostAddress().toString();
		    	String[] sub = value.toString().split("/");
		    	String tmpPath = "/home/hadoop/vinamap/data/"+sub[sub.length-1];
		    	//relative path  /huyangen/vina/input/3/c69
		    	//not absolute path "hdfs://192.168.30.42:9000/huyangen/vina/input/3/c69";
		    	hf.HadoopToLocal(path.substring(path.indexOf("9000/")+4), tmpPath);
		    	FileOperation fo = new FileOperation();

		    	String conf2="/home/hadoop/vinamap/conf2";
		    	String receptor="/home/hadoop/vinamap/2RH1C2.pdbqt";
		    	String ligand="/home/hadoop/vinamap/data/"+sub[sub.length-1];
		    		//	"/home/hadoop/data/ZINC14684865.pdbqt";
		    	String out="/home/hadoop/vinamap/out/"+sub[sub.length-1];
		    	fo.createDir("/home/hadoop/vinamap/out/");
		    	String log="/home/hadoop/vinamap/out/"+sub[sub.length-1]+".log";
		    	String vinacommand="/usr/local/cloud/vina/autodock_vina_1_1_2_linux_x86/bin/vina --config "+conf2+" --ligand "+ligand+" --receptor "+receptor+" --seed 1351189036 "+" --out "+out+" --log "+log;
		    	System.out.println(vinacommand);
		    	
		    	
		    	Process vinaPro=Runtime.getRuntime().exec(vinacommand);
		    	vinaPro.waitFor();
		    	hf.localToHadoop(log, "/huyangen/vinaResult/JobID/"+jobID+"/out/"+sub[sub.length-1]+".log");
		    	hf.localToHadoop(out, "/huyangen/vinaResult/JobID/"+jobID+"/out/"+sub[sub.length-1]);
		    	fo.deleteFile(ligand);
		    	fo.deleteFile(log);
		    	fo.deleteFile(out);
		    	word.set(ip);
		    	try {
					context.write(word,new Text(hf.readFromHadoop(path)));
				} catch (Exception e) {
					e.printStackTrace();
				}
		    }
		  }
		  
		  public  static class VinaReducer extends Reducer<Text,Text,Text,Text> {
			  public void reduce(Text key, Iterable<Text> values, 
	                      Context context
	                      ) throws IOException, InterruptedException {
				  for (Text val : values) {
					  context.write(key, val);
				  }
			  }
		  }
	
	public void iniJob(String jobPath,String dataPath,String jobID,int bucket) throws IOException{
		
		//String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";
       // String dataPath = "hdfs://192.168.30.42:9000/huyangen/vina/input";
		GeneratePath gp = new GeneratePath(jobPath,dataPath);
		ArrayList<String> test =new ArrayList<String>();
		//test.add("/1");
		//test.add("/2");
		//test.add("/3");
		test.add("/6");
		gp.createMeta(test, jobID, bucket);
	}
	
	public void iniJob(String jobPath,String dataPath,String jobID) throws IOException{
		iniJob(jobPath,dataPath,jobID,3);
	}
	
	public void startJob(String input ,String output,String jobID) throws IOException, ClassNotFoundException, InterruptedException{
		//final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";
        //final String dataPath = "hdfs://192.168.30.42:9000/huyangen/vina/input";
		//String jobID = "20130826";
		//int bucket = 20;
        //String jobID = args[0];
		//final String input = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/metadata";
		//final String output = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/output";
		
		 Configuration conf =(new HadoopConf()).getConf();
		 conf.set("jobID", jobID);
		 conf.set("conf2", "/huyangen/vina/conf2");
		 conf.set("receptor","/huyangen/vina/2RH1C2.pdbqt");
		 FileSystem fs = FileSystem.get(conf);
		 Path path = new Path(output);
		 if(fs.exists(path)){
		    fs.delete(path);
		   }	 
		    Job job = new Job(conf, "huyangen Vina job");
		    JobConf confs = new JobConf();  
		    confs.setJar("janus.jar"); 
		    job.setJarByClass(VinaJob.class);
		    job.setMapperClass(VinaMapper.class);
		    job.setCombinerClass(VinaReducer.class);
		    job.setReducerClass(VinaReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(input));
		    FileOutputFormat.setOutputPath(job, new Path(output));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
