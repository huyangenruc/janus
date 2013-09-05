package cn.bcc.util;

import cn.bcc.meta.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * implement file operation on hadoop,eg:read,write,delete
 * @author hu
 *
 */

public class HadoopFile {
	
	private static  Configuration conf = null;
	public HadoopFile() throws IOException{
		conf = (new HadoopConf()).getConf();
	}
/**
 * 
 * @param path: relative path on hdfs,eg: "/hadoop"
 * @return
 * @throws Exception
 */
	public String readFromHadoop(String path) throws Exception{   
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(path);
		if(fs.exists(src)){
			FSDataInputStream is = fs.open(src);
			FileStatus stat = fs.getFileStatus(src);
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0, buffer);
			is.close();
			//fs.close();
			return (new String(buffer));
		}else{
			throw new Exception("the file is not found");
		}
	}
	public void writeToHadoop(String path,ArrayList<String> al) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(path);
		FSDataOutputStream os = fs.create(src);
		for (int i=0;i<al.size();i++){
			os.write((al.get(i)+"\n").getBytes());
		}
		os.close();
	}
	
	 public  void createNewHDFSFile(String toCreateFilePath, String content) throws IOException
	    {
		 FileSystem fs = FileSystem.get(conf);   
	     FSDataOutputStream os = fs.create(new Path(toCreateFilePath));
	     os.write(content.getBytes("UTF-8"));
	     os.close();
	    }
/**
 * 
 * @param src: local file absoulute path
 * @param des: destination absolute path,not including hdfs://192.168.30.42:9000
 * @throws IOException
 */
	
	public boolean localToHadoop(String src,String des) throws IOException{
		boolean flag = false;
		FileSystem fs = FileSystem.get(conf);
		Path desPath = new Path(des);
		Path srcPath = new Path(src);
		fs.copyFromLocalFile(srcPath, desPath);
		flag = fs.exists(desPath);
		return flag;
	}
	
	
	//linux version
	public void HadoopToLocal(String hadoop,String local) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		File file = new File(local);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}
		/*if(file.exists()){
			file.delete();
		}*/
		Path hadoopPath = new Path(hadoop);
		Path localPath = new Path(local);
		fs.copyToLocalFile(hadoopPath, localPath);
	}
	
	

	/**
	 * 
	 * @param hadoopDir :file directory path on hdfs 
	 * @param localDir : file directory path on local file system ,eg /home/hadoop 
	 * @return 
	 * @throws IOException
	 */
	public boolean getFromHadoop(String hadoopDir,String localDir) throws IOException{
		if(hadoopDir==null||localDir == null){
			return false;
		}
		if(localDir.charAt(localDir.length()-1)=='/'){
			localDir = localDir.substring(0, localDir.length()-1);
		}
		
		
		boolean flag = false;
		File fileDir = new File(localDir);
		if(!fileDir.exists()){
			fileDir.mkdirs();
		}
		FileSystem fs = FileSystem.get(conf);
		Path hadoopPath = new Path(hadoopDir);
		FileStatus[] stats = fs.listStatus(hadoopPath);
	
        for(int i=0;i<stats.length;i++){
        	if(!stats[i].isDir()){
        		FSDataInputStream in = fs.open(stats[i].getPath()); 	
        		FileOutputStream fos = new FileOutputStream(localDir+"/"+stats[i].getPath().getName());
                int bytesRead;
                byte[] buffer = new byte[4096];
                while ((bytesRead = in.read(buffer)) > 0) {
                    fos.write(buffer, 0, bytesRead);
                }   		 		
                in.close();
                fos.close();
                flag = true;
        	}else{
        		String newHadoopDir = stats[i].getPath().toString();
        		String newLocalDir = localDir+"/"+stats[i].getPath().getName();
        		flag = getFromHadoop(newHadoopDir,newLocalDir);
        	}
        }	
		return flag;
	}
	
	public boolean exportFile(String vinaJobID,String option,String localPath) throws IOException{
			boolean flag = false;
			String path = "/vinaResult/vinaJobID/"+vinaJobID;
			if(option.equals("exception")){
				Path hdfsPath = new Path(path+"/exception");
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] stats = fs.listStatus(hdfsPath);
				if(stats==null){
					flag = false;
					System.out.println("no exception file");
				}else{
					flag = getFromHadoop(path+"/exception",localPath+"/exception");
					fs.delete(hdfsPath);
				}
			}else if(option.equals("result")){
				flag = getFromHadoop(path+"/result",localPath);
				flag = getFromHadoop(path+"/order",localPath);
			}else{
				
			}
			return flag;
			
		}
	
/*
 * delete file or directory
 * @param file or directory absolute path;
 */
	public boolean exist(String path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(path);
		return fs.exists(src);
		
	}
	@SuppressWarnings("deprecation")
	public boolean delete(String path) throws IOException{
		if(path==null){
			return false;
		}	
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(path);
		boolean success = false;
		success = fs.delete(src);
		return success;
	}
	@SuppressWarnings("deprecation")
	public ArrayList<String> listAll(String dir) throws IOException{
		ArrayList<String> al = new ArrayList<String>();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(dir);
		
		if(fs.isDirectory(path)){
			FileStatus[] stats = fs.listStatus(path);
			for(int i=0;i<stats.length;i++){	
				al.add(stats[i].getPath().toString());
			}
		}
		return al;
	}
	
	public ArrayList<String> listChild(String parentDir) throws IOException{
		ArrayList<String> al = new ArrayList<String>();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(parentDir);
		FileStatus[] stats = fs.listStatus(path);
		for(int i=0;i<stats.length;i++){
			String tmpPath = stats[i].getPath().toString();
			al.add(tmpPath.substring(tmpPath.lastIndexOf("/")));
		}
		return al;
	}
	
	
	
}
