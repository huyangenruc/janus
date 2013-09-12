package cn.bcc.meta;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;


/**
 * hadoop conf infomation
 * @author hu
 */
public class HadoopConf {
    private static Configuration conf = null;
    final String fsAddress = "hdfs://192.168.30.42:9000/";
    final String jobTrackerAddress = "192.168.30.41:9001";

    public HadoopConf() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", fsAddress);
        conf.set("mapred.job.tracker", jobTrackerAddress);
        conf.set("hadoop.job.user", "hadoop");
    }

    public Configuration getConf() {
        return conf;
    }
}
