package cn.bcc.core;

import cn.bcc.meta.*;
import cn.bcc.util.*;
import cn.bcc.exception.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VinaHadoop {

	  public static class VinaMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	    //private Text word = new Text();
	    private DoubleWritable word = new DoubleWritable();
	    public void setup(Context context) throws IOException, InterruptedException{
	    	Configuration conf = context.getConfiguration();
	    	String conf2 = conf.get("conf2");
	    	String receptor = conf.get("receptor");
	    	HadoopFile hf = new HadoopFile();
	    	hf.HadoopToLocal(conf2, "/home/hadoop/vinamap/"+conf2.substring(conf2.lastIndexOf("/")+1));
	    	hf.HadoopToLocal(receptor, "/home/hadoop/vinamap/"+receptor.substring(receptor.lastIndexOf("/")+1));
	    }
	    
	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
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
    	
	    	String seed = "1351189036";
	    	String exceptionPath = "/huyangen/vinaResult/JobID/"+jobID+"/exception/"+sub[sub.length-1]+".exception";
	    	RunVina vinajob = new RunVina(conf2,ligand,receptor,seed,out,log,path);
	    	String flag = vinajob.runVina();
	    	
	    	if("success".equals(flag)){
	    		String logHdfsPath = "/huyangen/vinaResult/JobID/"+jobID+"/out/"+sub[sub.length-1]+".log";
	    		String outHdfsPath = "/huyangen/vinaResult/JobID/"+jobID+"/out/"+sub[sub.length-1];		
	    				
	    		hf.localToHadoop(log,logHdfsPath);
		    	hf.localToHadoop(out,outHdfsPath );
		    	fo.deleteFile(ligand);
		    	fo.deleteFile(log);
		    	fo.deleteFile(out);
		    	String result;
				try {
					result = hf.readFromHadoop(outHdfsPath);	
					word.set(Double.parseDouble(result.split("\n")[1].split("    ")[1].trim()));
					context.write(word,new Text("hdfs://192.168.30.42:9000"+outHdfsPath));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}		  	    	
	    	}else{
	    		hf.createNewHDFSFile(exceptionPath, flag);
	    	}    	
	    }
	  }
	  
	  public  static class VinaReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
		  public void reduce(DoubleWritable key, Iterable<Text> values, 
                      Context context
                      ) throws IOException, InterruptedException {
			  for (Text val : values) {
				  context.write(key, val);
			  }
		  }
	  }
	  
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		Configuration conf =(new HadoopConf()).getConf();
		FileSystem fs = FileSystem.get(conf);
		final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";     
		//final String dataPath = "hdfs://192.168.30.42:9000/huyangen/vina/input";
		final String dataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data";  
		
		//String jobID = "20130826";
		//int bucket = 20;
        String jobID = args[0];
        int bucket = Integer.parseInt(args[1]);
        GeneratePath gp = new GeneratePath(jobPath,dataPath);
		/*ArrayList<String> test =new ArrayList<String>();
		test.add("/pdbqt_1");
		test.add("/pdbqt_10");
		test.add("/pdbqt_100");	*/
        HadoopFile hf = new HadoopFile();
        ArrayList<String> test = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
		gp.createMeta(test, jobID, bucket);
    
		final String input = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/metadata";
		final String output = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/output";
		
		
		conf.set("jobID", jobID);
		 
		 
		conf.set("conf2", "/huyangen/vina/conf2");
		conf.set("receptor","/huyangen/vina/2RH1C2.pdbqt");
		String[] otherArgs = {input,output};
		
		Path path = new Path(otherArgs[1]);
		if(fs.exists(path)){
		   fs.delete(path);
		  }
		 
		Job job = new Job(conf, "Vina Hadoop");
		JobConf confs = new JobConf();  
		confs.setJar("janus.jar"); 
		job.setJarByClass(VinaHadoop.class);
		job.setMapperClass(VinaMapper.class);
		job.setCombinerClass(VinaReducer.class);
		job.setReducerClass(VinaReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
