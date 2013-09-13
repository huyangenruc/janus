package cn.bcc.util;

import java.io.IOException;
import java.util.ArrayList;

/**
 * this class will generate metadata(vina data directory infomation) for vina job
 * 
 * @author hu
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
     * @param relativePath
     * @param jobID
     * @param node
     * @throws IOException
     */
    public void createMeta(ArrayList<String> relativePath, String jobID, int node)
            throws IOException {
        if (relativePath == null || relativePath.size() == 0 || jobID == null || node <= 0
                || node > 1000) {
            return;
        }
        int buckets = 0;
        HadoopFile operation = new HadoopFile();
        ArrayList<String> absolutePath = getHdfsPath(relativePath);
        ArrayList<String> items = new ArrayList<String>();
        for (int i = 0; i < absolutePath.size(); i++) {
            ArrayList<String> al = operation.listAll(absolutePath.get(i));
            items.addAll(al);
        }

        if (operation.exist(jobPath + jobID)) {
            operation.delete(jobPath + jobID);
        }
        if (items.size() == 0) {
            return;
        }

        if (items.size() > 0 && items.size() < 50) {
            buckets = 1;
            ArrayList<String> sub = new ArrayList<String>();
            sub.addAll(items);
            operation.writeToHadoop(jobPath + jobID + "/metadata/" + (new Integer(0)).toString(),
                    sub);
            return;
        } else if (items.size() >= 50 && items.size() < 500) {
            buckets = 9;
        } else {
            buckets = node * 3;
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
