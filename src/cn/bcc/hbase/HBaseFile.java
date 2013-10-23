package cn.bcc.hbase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import cn.bcc.meta.LigandPool;

public class HBaseFile implements LigandPool{
    
    private static Configuration config;
    private static HBaseAdmin admin;
    private HTable table;
    
    
    public HBaseFile() throws IOException {
        config = HBaseConfiguration.create();
        admin = new HBaseAdmin(config);
    }

    
    @Override
    public String readLigand(String path) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * ligandPath: 
     * format:tableName/rowKey
     */
    @Override
    public void copyToLocal(String ligandPath, String ligandLocal) {
        String[] args = ligandPath.split("/");
        String tableName = args[0];
        String rowKey = args[1];
        try {
            table = new HTable(config,tableName);
            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询  
            Result r = table.get(scan);  
            if(r.raw().length==1){
                KeyValue keyValue = r.raw()[0];
              /*  System.out.println("列：" + new String(keyValue.getFamily())  
                + "====值:" + "\n"+new String(keyValue.getValue())); */
                File file = new File(ligandLocal);
                if(!file.getParentFile().exists()){
                    file.getParentFile().mkdirs();
                }
                FileOutputStream fos = new FileOutputStream(ligandLocal);
                fos.write(keyValue.getValue());
                fos.close();           
            }
           /* for (KeyValue keyValue : r.raw()) {  
                System.out.println("列：" + new String(keyValue.getFamily())  
                        + "====值:" + "\n"+new String(keyValue.getValue()));  
            }  */
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
