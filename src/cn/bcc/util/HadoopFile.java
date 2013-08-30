package cn.bcc.util;

import cn.bcc.meta.*;

import java.io.File;
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
	
	public void localToHadoop(String src,String des) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		
		Path desPath = new Path(des);
		Path srcPath = new Path(src);
		fs.copyFromLocalFile(srcPath, desPath);
	}
	
	public void HadoopToLocal(String hadoop,String local) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		File file = new File(local);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}
		if(file.exists()){
			file.delete();
		}
		Path hadoopPath = new Path(hadoop);
		Path localPath = new Path(local);
		fs.copyToLocalFile(hadoopPath, localPath);
	}
	
	
/*
 * delete file or directory
 * @para file or directory absolute path;
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
