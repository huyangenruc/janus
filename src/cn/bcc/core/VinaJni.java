package cn.bcc.core;

public class VinaJni {
	//String conf,protein,element;
	public native String getVinaResult(String config,String ligand,String receptor,int seed);
	static
	{
		System.loadLibrary("vinajni.so");
	}
	/*public String getJavaVinaResult(String config,String ligand,String receptor,int seed){
		return config+ligand+ligand;
	}*/
}
