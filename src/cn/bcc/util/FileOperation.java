package cn.bcc.util;

/**
 * java file operation,including add,delete file,directory...
 */

import java.io.*;

public class FileOperation {
	
	
/*
 * create a new file
 * path exap: "/home/hadoop/data/a/a.pdbqt"
 */
	public void createFile(String path) throws IOException{
		File file = new File(path);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}		
		if(!file.exists()){
			file.createNewFile();
		}else{
			file.delete();
			file.createNewFile();
		}
	}

/**
 * 
 * @param path
 * @throws IOException
 */
	public void createDir(String path) throws IOException{
		File file = new File(path);
		if(!file.exists()){
    		file.mkdirs();
    	}
	}
	
	public void deleteFile(String path){
		File file = new File(path);
		if(file.exists()){
			file.delete();
		}
		
	}
	
	 public static boolean deleteDir(File dir) {
	        if (dir.isDirectory()) {
	            String[] children = dir.list();
	            for (int i=0; i<children.length; i++) {
	                boolean success = deleteDir(new File(dir, children[i]));
	                if (!success) {
	                    return false;
	                }
	            }
	        }
	        return dir.delete();
	    }
	
}
