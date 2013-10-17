package cn.bcc.core;

import java.io.IOException;
import java.util.ArrayList;

import cn.bcc.util.HadoopFile;

/**
 * this class will generate metadata(vina data directory infomation) for vina job
 * 
 * @author huyangen
 * 
 */

public class GeneratePath {
    String jobPath = null;
    String dataPath = null;

    public GeneratePath(String jobPath, String dataPath) {
        this.jobPath = jobPath;
        this.dataPath = dataPath;
    }

    /**
     * create metadata info
     * 
     * @param relativePath
     * @param jobID
     * @param node
     * @throws IOException
     */
    public void createMeta(ArrayList<String> items, String jobID, int node) throws IOException {
        if (items == null || items.size() == 0 || jobID == null || node <= 0 || node > 1000) {
            return;
        }
        int buckets = 0;
        HadoopFile operation = new HadoopFile();
        if (operation.exist(jobPath + jobID)) {
            operation.delete(jobPath + jobID);
        }
        int length = items.size();
        if (length == 0) {
            return;
        }

        if (length > 0 && length < 50) {
            buckets = 1;
            operation.writeToHadoop(jobPath + jobID + "/metadata/" + (new Integer(0)).toString(),
                    items);
            return;
        } else if (length >= 50 && length < 500) {
            buckets = 6;
        } else {

            buckets = length / 100;
        }
        int count = items.size() / buckets;
        for (int i = 0; i < buckets - 1; i++) {
            ArrayList<String> sub = new ArrayList<String>();
            sub.addAll(items.subList(i * count, (i + 1) * count));
            operation.writeToHadoop(jobPath + jobID + "/metadata/" + (new Integer(i)).toString(),
                    sub);
        }
        ArrayList<String> sub = new ArrayList<String>();
        sub.addAll(items.subList((buckets - 1) * count, items.size()));
        operation.writeToHadoop(
                jobPath + jobID + "/metadata/" + (new Integer(buckets - 1)).toString(), sub);
        return;

    }

    /**
     * 
     * @param relativePath :generate absolute path for data in hdfs
     * @return hdfsPath,eg:hdfs://192.168.30.42:9000/data/...
     */
    private ArrayList<String> getHdfsPath(ArrayList<String> relativePath) {
        ArrayList<String> hdfsPath = new ArrayList<String>();
        for (String item : relativePath) {
            hdfsPath.add(dataPath + item);
        }
        return hdfsPath;
    }

}
