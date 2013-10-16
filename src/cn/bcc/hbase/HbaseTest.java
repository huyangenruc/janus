package cn.bcc.hbase;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import cn.bcc.meta.HadoopConf;
import cn.bcc.util.HadoopFile;

public class HbaseTest {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        HbaseOperation hb = new HbaseOperation("bcc_free");
        hb.createTable("bcc_free", "columnValue");
        //hb.putData("bccfree00000000.pdbqt", "columnValue", "value", "its just a test");
        Configuration conf =(new HadoopConf()).getConf();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("/bcc/data/bcc_free");
        List<Put> lp = new ArrayList<Put>();
        if (fs.exists(src)) {
            FileStatus[] status = fs.listStatus(src);
            for(int i=0;i<status.length;i++){
                System.out.println(status[i].getPath().getName());
                FSDataInputStream is = fs.open(status[i].getPath());
                FileStatus stat = fs.getFileStatus(status[i].getPath());
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                is.readFully(0, buffer);
                is.close();

                String rowKey = status[i].getPath().getName();
                Put p = new Put(Bytes.toBytes(rowKey));
                p.add(Bytes.toBytes("columnValue"), Bytes.toBytes("value"), buffer);
                lp.add(p);
                if(lp.size()==400){
                    System.out.println(lp.size());
                    hb.putData(lp);
                    lp.clear();
                    System.out.println(lp.size());
                }
            }
        }
        if(lp.size()!=0){
            System.out.println(lp.size());
            hb.putData(lp);
            lp.clear();
        }
        
    }
    

}
