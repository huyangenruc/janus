package cn.bcc.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import cn.bcc.exception.RunVina;
import cn.bcc.util.FileOperation;
import cn.bcc.util.HadoopFile;
import cn.bcc.vina.DataPair;

public  class VinaMapper extends Mapper<Object, Text, DoubleWritable, DataPair> {
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
        k = conf.getInt("k", 100);
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
            DataPair data =
                    new DataPair(Double.parseDouble(result.split("\n")[1].split("    ")[1]
                            .trim()), outHdfsPath, result, logHdfsPath, logInfo);
            tree.add(data);
            if (tree.size() > k) {
                tree.pollLast();
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