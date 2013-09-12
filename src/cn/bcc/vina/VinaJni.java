package cn.bcc.vina;

public class VinaJni {
    // String conf,protein,element;
    public native String getVinaResult(String config, String ligand, String receptor, String seed);

    static {
        System.loadLibrary("vinajni.so");
    }
    /*
     * public String getJavaVinaResult(String config,String ligand,String receptor,int seed){ return
     * config+ligand+ligand; }
     */
}
