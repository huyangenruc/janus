package cn.bcc.util;

import java.util.ArrayList;
import cn.bcc.meta.*;
import java.io.*;

public class HadoopFileTest {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//HadoopFile file = new HadoopFile();
		//System.out.println(file.delete("hdfs://192.168.30.42:9000/huyangen/vina/output/part-r-00000"));
		//System.out.println(file.readFromHadoop("hdfs://192.168.30.42:9000/huyangen/vina/input/1/a1"));
		//System.out.println(file.readFromHadoop("hdfs://192.168.30.42:9000/huyangen/vina/input/2/b1"));
		//System.out.println(file.delete("hdfs://192.168.30.42:9000/huyangen/pathInfoB"));
		//file.listAll("hdfs://192.168.30.42:9000/huyangen/vina/input/1");
		//file.listAll("hdfs://192.168.30.42:9000/huyangen/input");
		//ArrayList<String> al =new ArrayList<String>();
		//al.add("hdfs://192.168.30.42:9000/huyangen/input");
	//	al.add("hdfs://192.168.30.42:9000/huyangen/input/1");
		//file.writeToHadoop("hdfs://192.168.30.42:9000/huyangen/test", al.get(0).getBytes());
		//file.writeToHadoop("hdfs://192.168.30.42:9000/huyangen/test", al);
		//file.writeToHadoop("hdfs://192.168.30.42:9000/huyangen/test",arr);
		//System.out.println(file.readFromHadoop("hdfs://192.168.30.42:9000/huyangen/test"));
		//System.out.println((ArrayList<String>)al.subList(0, al.size()-1));
	
	
	/*	GeneratePath gp=new GeneratePath();
		ArrayList<String> test =new ArrayList<String>();
		test.add("/1");
		test.add("/2");
		test.add("/3");
		test.add("/4");
		gp.createMeta(test, "20130823", 2);*/
		FileOperation fo = new FileOperation();
	
		fo.createFile("C:/Users/hu/Desktop/a");
		HadoopFile hf = new HadoopFile();
		hf.localToHadoop("C:/Users/hu/Desktop/a", "/");
		
		//File file = new File("C:/Users/hu/Desktop/FileTest/b/test.txt");
		//System.out.println(file.getParent());
		//System.out.println(file.getParentFile().isDirectory());
		
	}

}
