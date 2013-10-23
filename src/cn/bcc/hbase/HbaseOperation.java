package cn.bcc.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperation {
    private static Configuration config;
    private static HBaseAdmin admin;
    private static HTable table;
    

/*    public HbaseOperation() throws MasterNotRunningException, ZooKeeperConnectionException {
        config = HBaseConfiguration.create();
        admin = new HBaseAdmin(config);
    }*/
    
    public HbaseOperation(String tableName) throws IOException {
        config = HBaseConfiguration.create();
        admin = new HBaseAdmin(config);
        table = new HTable(config,tableName);
    }

    /**
     * create a new hbase table
     * @param tableName
     * @param HColumnName
     * @return
     * @throws IOException
     */
    public boolean createTable(String tableName, String HColumnName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println("table:"+tableName+"already exist");
            return false;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor cf = new HColumnDescriptor(HColumnName);
        tableDesc.addFamily(cf);
        admin.createTable(tableDesc);
        return true;
    }

    public void addColumn(String tableName, String HColumnName) throws IOException {
        HColumnDescriptor column = new HColumnDescriptor(HColumnName);
        admin.addColumn(tableName, column);
    }
    
    public void getData(String rowKey) throws IOException{
        Get get = new Get();
        Result result=table.get(get);
    }
    /**
     * put a single record to hbase table
     * 
     * @param rowKey
     * @param ColumnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public void putData(String rowKey,String ColumnFamily,String column,String value) throws IOException{
        Put p = new Put(Bytes.toBytes(rowKey));
        p.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(p);
    }
    /**
     * put record list to hbase table
     * @param lp
     * @throws IOException
     */
    public void putData(List<Put> lp) throws IOException{
        //Put p = new Put(Bytes.toBytes("rowKey"));
        //p.add(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        table.put(lp);
    }
    
    public void queryAll(){
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====值:" +"\n"+ new String(keyValue.getValue()));  
                }  
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }
    
    public void query(String rowKey){
       
        try {  
            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询  
            Result r = table.get(scan);  
            System.out.println("获得到rowkey:" + new String(r.getRow()));  
            for (KeyValue keyValue : r.raw()) {  
                System.out.println("列：" + new String(keyValue.getFamily())  
                        + "====值:" + "\n"+new String(keyValue.getValue()));  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
            
        }  
    }
    

}
