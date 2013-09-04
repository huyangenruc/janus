package cn.bcc.vina;

import cn.bcc.meta.*;
import cn.bcc.util.*;
import cn.bcc.exception.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

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

/**
 * 
 * @author huyangen
 *
 */
public class VinaHadoop {

	  public static class VinaMapper extends Mapper<Object, Text, DoubleWritable, DataPair>{
	    private int k;
	    private String jobID;
        private String conf2HDFS;
        private String conf2Local;
        private String seed;
        private String receptorHDFS;
        private String receptorLocal;
        private String tmpPath;
	    private HadoopFile hf;
	    private FileOperation fo ;
	    private TreeSet<DataPair> tree = new TreeSet<DataPair>();
	    Configuration conf;
	    /*
	     * (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    public void setup(Context context) throws IOException, InterruptedException{
	    	conf = context.getConfiguration();
	    	conf2HDFS = conf.get("conf2HDFS");
	    	receptorHDFS = conf.get("receptorHDFS");
	    	seed = conf.get("seed");
	    	jobID = conf.get("jobID");
	    	k = conf.getInt("k", 100);
	    	hf = new HadoopFile();
	    	fo = new FileOperation();
	    	tmpPath = fo.randomString();
	    	conf2Local = "/home/hadoop/"+tmpPath+"/"+conf2HDFS.substring(conf2HDFS.lastIndexOf("/")+1);
	    	receptorLocal = "/home/hadoop/"+tmpPath+"/"+receptorHDFS.substring(receptorHDFS.lastIndexOf("/")+1);
	    	hf.HadoopToLocal(conf2HDFS, conf2Local);
	    	hf.HadoopToLocal(receptorHDFS, receptorLocal);
	    }
	    /*
	     * (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	String path = value.toString();
	    	String[] sub = value.toString().split("/");
	    	String ligandName = sub[sub.length-1];
	    	String ligandLocal = "/home/hadoop/"+tmpPath+"/data/"+ligandName;
	    	hf.HadoopToLocal(path, ligandLocal);
	    	String out="/home/hadoop/"+tmpPath+"/out/"+ligandName;
	    	fo.createDir("/home/hadoop/"+tmpPath+"/out/");
	    	String log="/home/hadoop/"+tmpPath+"/out/"+ligandName+".log";
	    	String exceptionPath = "/huyangen/vinaResult/JobID/"+jobID+"/exception/"+ligandName+".exception";
	    	RunVina vinajob = new RunVina(conf2Local,ligandLocal,receptorLocal,seed,out,log,path);
	    	String flag = vinajob.runVina();
	    	if("success".equals(flag)){
	    		String logHdfsPath = "/huyangen/vinaResult/JobID/"+jobID+"/result/"+ligandName+".log";
	    		String outHdfsPath = "/huyangen/vinaResult/JobID/"+jobID+"/result/"+ligandName;		
		    	String result = fo.readFile(out);
		    	String logInfo = fo.readFile(log);
		    	DataPair data = new DataPair(Double.parseDouble(result.split("\n")[1].split("    ")[1].trim()),outHdfsPath,result,logHdfsPath,logInfo);
		    	tree.add(data);
		    	if(tree.size()>k){
		    		tree.pollLast();
		    	}
	    	}else{
	    		hf.createNewHDFSFile(exceptionPath, flag);
	    	}    	
	    }
	    
	    protected void cleanup(Context context) throws IOException,  InterruptedException {
	    	Iterator<DataPair> it = tree.iterator();
	    	while(it.hasNext()){
	    		DataPair dp = it.next();
	    		context.write(new DoubleWritable(dp.getLigandDouble()), dp);
	    	}
	    	fo.deleteDir(new File("/home/hadoop/"+tmpPath));
	  }	    
	 }
	  
	  public  static class VinaReducer extends Reducer<DoubleWritable,DataPair,DoubleWritable,Text> {
		  private int k ;
		  private int count = 0;
		  private HadoopFile hf;
		  protected void setup(Context context
                  ) throws IOException, InterruptedException {
			  Configuration conf = context.getConfiguration();
		    	hf = new HadoopFile();
		    	k = conf.getInt("k", 50); 
		  }
		  
		  public void reduce(DoubleWritable key, Iterable<DataPair> values, 
                  Context context
                  ) throws IOException, InterruptedException {
		  for (DataPair val : values) {
			  if(count<k){
				hf.createNewHDFSFile(val.getLigandPath(), val.getVinaResult());
				hf.createNewHDFSFile(val.getLogPath(), val.getVinaLog());
		    	context.write(new DoubleWritable(val.getLigandDouble()),new Text(val.getLigandPath()));
		    	count++;
			  }else{
				  break;			  
			  }
		  }	  
	  }
	 }
	  
	  
	  private void iniJob(String jobPath,String dataPath,String jobID,ArrayList<String> al,int bucket) throws IOException{
		  GeneratePath gp = new GeneratePath(jobPath,dataPath);
		  gp.createMeta(al, jobID, bucket);
	  }
	  
	  public void startJob(String jobPath,String dataPath,String jobID,ArrayList<String> al,int bucket,int topK) throws IOException, ClassNotFoundException, InterruptedException{
		  iniJob(jobPath,dataPath,jobID,al,bucket);
		  Configuration conf =(new HadoopConf()).getConf();
			FileSystem fs = FileSystem.get(conf);
			iniJob(jobPath,dataPath,jobID,al,bucket);
			final String input = jobPath+jobID+"/metadata";
			final String output = jobPath+jobID+"/output";	
			conf.set("jobID", jobID);
			conf.setInt("k", topK);
			conf.set("conf2HDFS", "/huyangen/vina/conf2");
			conf.set("receptorHDFS","/huyangen/vina/2RH1C2.pdbqt");
			conf.set("seed", "1351189036");
			Path path = new Path(output);
			if(fs.exists(path)){
				fs.delete(path);
				}
			Job job = new Job(conf, "vinaHadoop"+jobID);
			JobConf confs = new JobConf();  
			confs.setNumReduceTasks(1);
			confs.setJar("janus.jar"); 
			job.setJarByClass(VinaHadoop.class);
			job.setMapperClass(VinaMapper.class);
			job.setReducerClass(VinaReducer.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(DataPair.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(DataPair.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	  
	  
	/*public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		Configuration conf =(new HadoopConf()).getConf();
		FileSystem fs = FileSystem.get(conf);
		final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";     
		final String dataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data";  
        String jobID = args[0];
        int bucket = Integer.parseInt(args[1]);
        GeneratePath gp = new GeneratePath(jobPath,dataPath);
		ArrayList<String> test =new ArrayList<String>();
		test.add("/pdbqt_1");
		test.add("/pdbqt_10");
		test.add("/pdbqt_100");	
        HadoopFile hf = new HadoopFile();
        //ArrayList<String> test = hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
		gp.createMeta(test, jobID, bucket);
		final String input = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/metadata";
		final String output = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/"+jobID+"/output";	
		conf.set("jobID", jobID);
		conf.setInt("k", 100);
		conf.set("conf2HDFS", "/huyangen/vina/conf2");
		conf.set("receptorHDFS","/huyangen/vina/2RH1C2.pdbqt");
		conf.set("seed", "1351189036");
		String[] otherArgs = {input,output};
		Path path = new Path(otherArgs[1]);
		if(fs.exists(path)){
		   fs.delete(path);
		  }
		Job job = new Job(conf, "Vina Hadoop");
		JobConf confs = new JobConf();  
		confs.setNumReduceTasks(1);
		confs.setJar("janus.jar"); 
		job.setJarByClass(VinaHadoop.class);
		job.setMapperClass(VinaMapper.class);
		job.setReducerClass(VinaReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(DataPair.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DataPair.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/
}
