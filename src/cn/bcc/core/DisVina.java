package cn.bcc.core;

import cn.bcc.meta.*;
import cn.bcc.util.*;
import cn.bcc.vina.DataPair;
import cn.bcc.exception.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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
public class DisVina {

    public static class VinaMapper extends Mapper<Object, Text, NullWritable, DataPair> {
        private int k;
        private String conf2;
        private String receptor;
        private String seed;
        private String jobID;
        HadoopFile hf;
        private TreeSet<DataPair> tree = new TreeSet<DataPair>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            hf = new HadoopFile();
            jobID = conf.get("jobID");
            k = conf.getInt("k", 100);
            seed = conf.get("seed");
            try {
                conf2 = hf.readFromHadoop(conf.get("conf2"));
                receptor = hf.readFromHadoop(conf.get("receptor"));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String path = value.toString();
            String ligand = null;
            try {
                ligand = hf.readFromHadoop(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // initailize vina in jni way
            VinaJni vinaInstance = new VinaJni();
            String vinaResult = vinaInstance.getVinaResult(conf2, ligand, receptor, seed);
            // get ligand name
            String[] sub = value.toString().split("/");
            String ligandPath =
                    "/huyangen/vinaResult/JobID/" + jobID + "/result/" + sub[sub.length - 1];
            String logPath = null;
            String log = null;
            DataPair data =
                    new DataPair(Double.parseDouble(vinaResult.split("\n")[1].split("    ")[1]
                            .trim()), ligandPath, vinaResult, logPath, log);
            tree.add(data);

            if (tree.size() > k) {
                tree.pollLast();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            Iterator<DataPair> it = tree.iterator();
            while (it.hasNext()) {
                context.write(NullWritable.get(), it.next());
            }

        }
    }
    public static class VinaReducer extends Reducer<NullWritable, DataPair, DoubleWritable, Text> {

        public int k;
        private String jobID;
        HadoopFile hf;
        private TreeSet<DataPair> tree = new TreeSet<DataPair>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            hf = new HadoopFile();
            k = conf.getInt("k", 100);
            jobID = conf.get("jobID");
        }

        public void reduce(NullWritable key, Iterable<DataPair> values, Context context)
                throws IOException, InterruptedException {
            for (DataPair val : values) {
                tree.add(val);
                if (tree.size() > k) {
                    tree.pollLast();

                }
            }
            Iterator<DataPair> it = tree.iterator();
            while (it.hasNext()) {
                DataPair dp = it.next();
                String vinaResultPath = dp.getLigandPath();
                hf.createNewHDFSFile(vinaResultPath, dp.getVinaResult());
                context.write(new DoubleWritable(dp.getLigandDouble()), new Text(vinaResultPath));
            }
        }
    }


    public void iniJob(String jobPath, String dataPath, String jobID, ArrayList<String> al,
            int bucket) throws IOException {
        GeneratePath gp = new GeneratePath(jobPath, dataPath);
        gp.createMeta(al, jobID, bucket);
    }

    public void StartJob(String jobID, int bucket) throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = (new HadoopConf()).getConf();
        FileSystem fs = FileSystem.get(conf);
        final String input =
                "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/" + jobID + "/metadata";
        final String output =
                "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/" + jobID + "/output";
        conf.set("jobID", jobID);
        conf.setInt("k", 100);
        conf.set("conf2", "/huyangen/vina/conf2");
        conf.set("receptor", "/huyangen/vina/2RH1C2.pdbqt");
        conf.set("seed", "1351189036");
        Path path = new Path(output);
        if (fs.exists(path)) {
            fs.delete(path);
        }
        Job job = new Job(conf, "Vinajob" + jobID);
        JobConf confs = new JobConf();
        confs.setJar("janus.jar");
        job.setJarByClass(DisVina.class);
        job.setMapperClass(VinaMapper.class);
        job.setCombinerClass(VinaReducer.class);
        job.setReducerClass(VinaReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DataPair.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static void main(String[] args) throws ClassNotFoundException, IOException,
            InterruptedException, URISyntaxException {
        Configuration conf = (new HadoopConf()).getConf();
        FileSystem fs = FileSystem.get(conf);
        final String jobPath = "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/";
        // final String dataPath = "hdfs://192.168.30.42:9000/huyangen/vina/input";
        final String dataPath = "hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data";

        // String jobID = "20130826";
        // int bucket = 20;
        String jobID = args[0];
        int bucket = Integer.parseInt(args[1]);
        GeneratePath gp = new GeneratePath(jobPath, dataPath);
        /*
         * ArrayList<String> test =new ArrayList<String>(); test.add("/pdbqt_1");
         * test.add("/pdbqt_10"); test.add("/pdbqt_100");
         */
        HadoopFile hf = new HadoopFile();
        ArrayList<String> test =
                hf.listChild("hdfs://192.168.30.42:9000/usr/hadoop/bcc_test1_data");
        gp.createMeta(test, jobID, bucket);

        final String input =
                "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/" + jobID + "/metadata";
        final String output =
                "hdfs://192.168.30.42:9000/huyangen/vinaResult/JobID/" + jobID + "/output";


        conf.set("jobID", jobID);

        conf.set("k", "100");
        conf.set("conf2", "/huyangen/vina/conf2");
        conf.set("receptor", "/huyangen/vina/2RH1C2.pdbqt");
        conf.set("seed", "1351189036");
        String[] otherArgs = {input, output};

        Path path = new Path(otherArgs[1]);
        if (fs.exists(path)) {
            fs.delete(path);
        }

        Job job = new Job(conf, "Vina Hadoop");
        JobConf confs = new JobConf();
        confs.setJar("janus.jar");
        job.setJarByClass(DisVina.class);
        job.setMapperClass(VinaMapper.class);
        // job.setCombinerClass(VinaReducer.class);
        job.setReducerClass(VinaReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DataPair.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DataPair.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
