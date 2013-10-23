package cn.bcc.meta;

/**
 * 
 * @author huyangen this interface provates service for read ligand files,no matter the ligand files
 *         stores in hdfs,hbase or other platforms
 */
public interface LigandPool {
    /**
     * 
     * @param path
     * @return ligand file as string
     */
    public String readLigand(String path);

    /**
     * 
     * @param ligandPath : ligand path on hdfs,hbase or other platform
     * @param ligandLocal: copy ligand from hdfs or hbase to local 
     */
    public void copyToLocal(String ligandPath, String ligandLocal);
}
