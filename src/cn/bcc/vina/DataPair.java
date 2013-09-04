package cn.bcc.vina;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DataPair implements WritableComparable<DataPair> {
	Double ligandDouble;
	String ligandPath;
	String vinaResult;
	String logPath;
	String vinaLog;
	public DataPair(){
	}
	public DataPair(Double ligandDouble,String ligandPath,String vinaResult,String logPath,String vinaLog){
		this.ligandDouble = ligandDouble;
		this.ligandPath = ligandPath;
		this.vinaResult = vinaResult;
		this.logPath = logPath;
		this.vinaLog = vinaLog;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ligandDouble = in.readDouble();
		ligandPath = in.readUTF();
		vinaResult = in.readUTF();
		logPath = in.readUTF();
		vinaLog = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(ligandDouble);
		out.writeUTF(ligandPath);
		out.writeUTF(vinaResult);
		out.writeUTF(logPath);
		out.writeUTF(vinaLog);
	}

	public int compareTo(DataPair other) {
		// TODO Auto-generated method stub
		int i = ligandDouble.compareTo(other.ligandDouble);
		if(i!=0){
			return i;
		}else{
			return ligandPath.compareTo(other.ligandPath);
		}
		
	}
    
    public String getVinaResult(){
    	return vinaResult;
    }
    public String getLogPath(){
    	return logPath;
    }
    public String getVinaLog(){
    	return vinaLog;
    }
    
    public Double getLigandDouble(){
    	return ligandDouble;
    }
    
    public String getLigandPath() {
    	return ligandPath;
    }
	
    public String toString(){
    	return ligandDouble.toString()+ligandPath;
    }
	
}
