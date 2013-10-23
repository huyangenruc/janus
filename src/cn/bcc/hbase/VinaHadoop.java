package cn.bcc.hbase;

import cn.bcc.meta.HadoopConf;
import cn.bcc.util.*;
import cn.bcc.vina.DataPair;
import cn.bcc.exception.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
    final String jobPath = "hdfs://192.168.30.42:9000/vinaResult/vinaJobID/";
    final String srcDataPath = "hdfs://192.168.30.42:9000/bcc/data";

    public static class VinaMapper extends Mapper<Object, Text, DoubleWritable, DataPair> {
        private int k;
        private String vinaJobID;
        private String conf2HDFS;
        private String conf2Local;
        private String seed;
        private String receptorHDFS;
        private String receptorLocal;
        private String tmpPath;
        private HadoopFile hf;
        private FileOperation fo;
        private TreeSet<DataPair> tree = new TreeSet<DataPair>();
        Configuration conf;

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            conf2HDFS = conf.get("conf2HDFS");
            receptorHDFS = conf.get("receptorHDFS");
            seed = conf.get("seed");
            vinaJobID = conf.get("vinaJobID");
            k = conf.getInt("k", 1000);
            hf = new HadoopFile();
            fo = new FileOperation();
            tmpPath = fo.randomString();
            conf2Local =
                    "/home/hadoop/vinaJob/" + tmpPath + "/"
                            + conf2HDFS.substring(conf2HDFS.lastIndexOf("/") + 1);
            receptorLocal =
                    "/home/hadoop/vinaJob/" + tmpPath + "/"
                            + receptorHDFS.substring(receptorHDFS.lastIndexOf("/") + 1);
            hf.HadoopToLocal(conf2HDFS, conf2Local);
            hf.HadoopToLocal(receptorHDFS, receptorLocal);

        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         * org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String path = value.toString();
            String[] sub = value.toString().split("/");
            String ligandName = sub[sub.length - 1];
            String ligandLocal = "/home/hadoop/vinaJob/" + tmpPath + "/data/" + ligandName;
            hf.HadoopToLocal(path, ligandLocal);
            String out = "/home/hadoop/vinaJob/" + tmpPath + "/out/" + ligandName;
            fo.createDir("/home/hadoop/vinaJob/" + tmpPath + "/out/");
            String log = "/home/hadoop/vinaJob/" + tmpPath + "/out/" + ligandName + ".log";
            String exceptionPath =
                    "/vinaResult/vinaJobID/" + vinaJobID + "/exception/" + ligandName
                            + ".exception";
            String exceptionBackupPath =
                    "/vinaResult/vinaJobID/" + vinaJobID + "/exceptionBackup/" + ligandName
                            + ".exception";
            RunVina vinajob =
                    new RunVina(conf2Local, ligandLocal, receptorLocal, seed, out, log, path);
            String flag = vinajob.runVina();
            if ("success".equals(flag)) {
                String logHdfsPath =
                        "/vinaResult/vinaJobID/" + vinaJobID + "/result/" + ligandName + ".log";
                String outHdfsPath = "/vinaResult/vinaJobID/" + vinaJobID + "/result/" + ligandName;
                String result = fo.readFile(out);
                String logInfo = fo.readFile(log);
                try{
                    DataPair data =
                            new DataPair(Double.parseDouble(result.split("\n")[1].split("    ")[1]
                                    .trim()), outHdfsPath, result, logHdfsPath, logInfo);
                    tree.add(data);
                    if (tree.size() > k) {
                        tree.pollLast();
                    }
                }catch(Exception e){
                    hf.createNewHDFSFile(exceptionPath, path+" bad result");
                }
               
            } else {
                hf.createNewHDFSFile(exceptionPath, flag);
                hf.createNewHDFSFile(exceptionBackupPath, flag);
            }
        }

        // delete random directory
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<DataPair> it = tree.iterator();
            while (it.hasNext()) {
                DataPair dp = it.next();
                context.write(new DoubleWritable(dp.getLigandDouble()), dp);
            }
            fo.deleteDir(new File("/home/hadoop/vinaJob/" + tmpPath));
        }
    }


    /*
     * vina reduce class
     */
    public static class VinaReducer extends Reducer<DoubleWritable, DataPair, DoubleWritable, Text> {
        private int k;
        private int count = 0;
        private HadoopFile hf;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            hf = new HadoopFile();
            k = conf.getInt("k", 1000);
        }

        // topK sort,write the result to hdfs
        public void reduce(DoubleWritable key, Iterable<DataPair> values, Context context)
                throws IOException, InterruptedException {
            for (DataPair val : values) {
                if (count < k) {
                    hf.createNewHDFSFile(val.getLigandPath(), val.getVinaResult());
                    hf.createNewHDFSFile(val.getLogPath(), val.getVinaLog());
                    context.write(new DoubleWritable(val.getLigandDouble()),
                            new Text(val.getLigandPath()));
                    count++;
                } else {
                    break;
                }
            }
        }
    }

    /**
     * start a vina hadoop job
     * 
     * @param confLocalPath
     * @param receptorLocalPath
     * @param ligandPath
     * @param seed
     * @param topK
     * @param vinaJobID
     * @param node
     * @return
     */
    
    public HashMap<String, String> startJob(String confLocalPath, String receptorLocalPath,
            ArrayList<String> ligandPath, String seed, int topK, String vinaJobID, int numPerNode,boolean verbose) {
        HashMap<String, String> hm = new HashMap<String, String>();
        if (confLocalPath == null || receptorLocalPath == null || ligandPath == null || seed == null
                || vinaJobID == null || ligandPath.size() == 0 || topK < 0) {
            hm.put("flag", "false");
            hm.put("hadoopID", "null");
            hm.put("vinaJobID", vinaJobID);
            hm.put("log", "error arguments");
            return hm;
        }
        GeneratePath gp = new GeneratePath(jobPath, srcDataPath);
        String confName = confLocalPath.substring(confLocalPath.lastIndexOf("/"));
        String confHDFSPath = jobPath + vinaJobID + confName;
        String receptorName = receptorLocalPath.substring(receptorLocalPath.lastIndexOf("/"));
        String receptorHDFSPATH = jobPath + vinaJobID + receptorName;
        HadoopFile hf;
        final String input = jobPath + vinaJobID + "/metadata";
        final String output = jobPath + vinaJobID + "/order";
        Path path = new Path(output);
        Configuration conf;
        FileSystem fs;
        Job job;
        try {
            gp.createMeta(ligandPath, vinaJobID, numPerNode);
            hf = new HadoopFile();
            hf.mkdir(jobPath + "/" + vinaJobID + "/exception");
            hf.mkdir(jobPath + "/" + vinaJobID + "/exceptionBackup");
            hf.localToHadoop(confLocalPath, confHDFSPath);
            hf.localToHadoop(receptorLocalPath, receptorHDFSPATH);
            conf = (new HadoopConf()).getConf();
            fs = FileSystem.get(conf);
            // set heart beat time 45min
            long milliSeconds = 45 * 60 * 1000;
            conf.setLong("mapred.task.timeout", milliSeconds);
            conf.set("vinaJobID", vinaJobID);
            conf.setInt("k", topK);
            conf.set("conf2HDFS", confHDFSPath);
            conf.set("receptorHDFS", receptorHDFSPATH);
            conf.set("seed", seed);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            job = new Job(conf, vinaJobID);
            job.setNumReduceTasks(1);
            job.setJarByClass(VinaHadoop.class);
            job.setMapperClass(VinaMapper.class);
            job.setReducerClass(VinaReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DataPair.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            hm.put("flag", "false");
            hm.put("hadoopID", "null");
            hm.put("vinaJobID", vinaJobID);
            hm.put("log", e.getMessage());
            return hm;
        }

        try {
            if(verbose){
                //System.exit(job.waitForCompletion(true) ? 0 : 1);
                job.waitForCompletion(true);
            }else{
                job.submit(); 
            }
            
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            hm.put("flag", "false");
            hm.put("hadoopID", "null");
            hm.put("vinaJobID", vinaJobID);
            hm.put("log", e.getMessage());
            return hm;
        }
        hm.put("flag", "true");
        hm.put("hadoopID", job.getJobID().toString());
        hm.put("vinaJobID", vinaJobID);
        hm.put("log", "null");
        return hm;
    }  

    /**
     * 
     * @param confLocalPath conf local path
     * @param receptorLocalPath receptor local path
     * @param ligandPath liangdir path,format: /dirName type: arrayList<String>
     * @param seed seed info ,type:String
     * @param topK top k ,type: int
     * @param vinaJobID vina job ID ,type String
     * @return
     */
    public HashMap<String, String> startJob(String confLocalPath, String receptorLocalPath,
            String ligandPath, String seed, int topK, String vinaJobID,int numPerNode,boolean verbose) {
        ArrayList<String> al = readLigand(ligandPath);
        HashMap<String, String> hm = new HashMap<String, String>();
        if("true".equals(checkLigandPath(al).get("flag"))){
            return startJob(confLocalPath, receptorLocalPath, al, seed, topK, vinaJobID, numPerNode,verbose);
        }else{
            hm.put("flag", "false");
            hm.put("hadoopID", "null");
            hm.put("vinaJobID", vinaJobID);
            hm.put("log", "error arguments,check "+ligandPath);
            return hm;
        }
        
    }
    
    public HashMap<String, String> startJob(String confLocalPath, String receptorLocalPath,
        String ligandPath, String seed, int topK, String vinaJobID) {
        return startJob(confLocalPath, receptorLocalPath, ligandPath, seed, topK, vinaJobID,100,false);
    }
    
    
    
    
    public  ArrayList<String> readLigand(String ligandPath) {
        ArrayList<String> al = new ArrayList<String>();
        File file = new File(ligandPath);
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                al.add(line);
            }
            br.close();
            fr.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return al;
    }

    public  HashMap<String, String> checkLigandPath(ArrayList<String> al) {
        HashMap<String, String> hm = new HashMap<String, String>();
        hm.put("flag", "false");
        if (al.size() == 0) {
            String log = "ligandPath is empth file";
            System.out.println(" 文件内容为空，请检查ligandPath");
            hm.put("log", log);
            return hm;
        }

        Configuration conf;
        try {
            conf = (new HadoopConf()).getConf();
            FileSystem fs = FileSystem.get(conf);
            int i = 1;
            for (String path : al) {
                if(path.isEmpty()){
                    System.out.println(path + " 请检查ligandPath 第 " + i + "行数据,该行数据为空");
                    String log = "please check ligandPath ,lineNumber"+i+" is empty";
                    hm.put("log", log);
                    return hm;
                }
                Path src = new Path(path);
                if (!fs.exists(src)) {
                    System.out.println(path + " 文件不存在，请检查ligandPath 第 " + i + "行数据");
                    String log = "please check ligandPath ,lineNumber"+i+" file does not exist";
                    hm.put("log", log);
                    return hm;
                }
                i++;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            hm.put("log", e.toString());
            return hm;
        }
        hm.put("flag", "true");
        return hm;
    }

}
