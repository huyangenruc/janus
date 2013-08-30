package cn.bcc.meta;

/*
 * Vina job parameters 
 */
public class VinaJobPara {
	private String config,ligand,receptor;
	private int seed;
	public VinaJobPara(String config,String ligand,String receptor,int seed){
		this.setConfig(config);
		this.setLigand(ligand);
		this.setReceptor(receptor);
		this.setSeed(seed);
	}
	public  String getConfig() {
		return config;
	}
	public  void setConfig(String config) {
		this.config = config;
	}
	public String getReceptor() {
		return receptor;
	}
	public void setReceptor(String receptor) {
		this.receptor = receptor;
	}
	public String getLigand() {
		return ligand;
	}
	public void setLigand(String ligand) {
		this.ligand = ligand;
	}
	public int getSeed() {
		return seed;
	}
	public void setSeed(int seed) {
		this.seed = seed;
	}
	
}
