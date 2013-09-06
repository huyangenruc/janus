package cn.bcc.util;

import java.io.IOException;
import java.util.ArrayList;

/**
 * this class will generate metadata(vina data directory infomation) for vina job
 * @author hu
 *
 */

public class GeneratePath {
    String jobPath = null;
    String dataPath = null;
  
    public GeneratePath(String jobPath,String dataPath){
    	this.jobPath = jobPath;
    	this.dataPath = dataPath;
    }
    

/*
 * @para relativePath :vina data ,format: /directoryName
 * create metadata info
 */
	public void createMeta(ArrayList<String> relativePath,String jobID,int buckets) throws IOException{
		if(relativePath==null||relativePath.size()==0||jobID==null||buckets<0||buckets>100){
			return;
		}
		HadoopFile operation = new HadoopFile();
		ArrayList<String> absolutePath = getHdfsPath(relativePath);
		ArrayList<String> items = new ArrayList<String>();
		for (int i=0;i<absolutePath.size();i++){
			ArrayList<String> al = operation.listAll(absolutePath.get(i));
			items.addAll(al);
		}
		
		if(operation.exist(jobPath+jobID)){
			operation.delete(jobPath+jobID);
		}
		if(items.size()==0){
			return;
		}		
		if(items.size()>0&&items.size()<50){
			buckets=1;
			ArrayList<String> sub=new ArrayList<String>();
			sub.addAll(items);
			operation.writeToHadoop(jobPath+jobID+"/metadata/"+(new Integer(0)).toString(), sub);
		}else{
			int count = items.size()/buckets; 
			for(int i=0;i<buckets-1;i++){
				ArrayList<String> sub=new ArrayList<String>();
						sub.addAll(items.subList(i*count, (i+1)*count));
				operation.writeToHadoop(jobPath+jobID+"/metadata/"+(new Integer(i)).toString(), sub);
			}
			ArrayList<String> sub=new ArrayList<String>();
					sub.addAll(items.subList((buckets-1)*count, items.size()));
			operation.writeToHadoop(jobPath+jobID+"/metadata/"+(new Integer(buckets-1)).toString(), sub);
		}
	}
	
/**
 * 
 * @param relativePath :generate absolute path for data in hdfs
 * @return hdfsPath,eg:hdfs://192.168.30.42:9000/data/...
 */
    private ArrayList<String> getHdfsPath(ArrayList<String> relativePath){
    	ArrayList<String> hdfsPath = new ArrayList<String>();
    	for(String item:relativePath){
    		hdfsPath.add(dataPath+item);
    	}
    	return hdfsPath;
    }
	
    
    
	


}
