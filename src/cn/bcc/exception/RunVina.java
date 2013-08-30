package cn.bcc.exception;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RunVina{

	private String config_file = null;
	private String ligand_file = null;
	private String receptor_file = null;
	private String output_file = null;
	private String output_log = null;
	private String vina_command;
	private int  seed = 0;
	//private String exception_file;
	private String exception_content;
	private String ligand_file_source_path = null;
	
	public RunVina(String config_file, String ligand_file, String receptor_file, String seed,
			      String output_file, String output_log, String ligand_file_source_ptah){
		this.config_file = config_file;
		this.ligand_file = ligand_file;
		this.receptor_file = receptor_file;
		this.output_file = output_file;
		this.output_log = output_log;
		this.seed = Integer.parseInt(seed);
		this.ligand_file_source_path = ligand_file_source_ptah;
	}
	
	/**
	 * @param args
	 * 
	 */
	public String runVina() {
		
		if (!paramAnalyzing()){
			//parameters are wrong flag and write exception log;
			return writeException();
		}
		if (!runVinaThread()){
			//Exception: vina running time more then 40 minutes and write exception log;
			return writeException();
		}
		if (!checkResultFile()){
			//result file is not exist or file size is 0 byte and write exception log;
			return writeException();
		}
		System.out.println("vina run sccess.");
		return "success";
		
		
	}
	/**
	 * deal with parameter 
	 * @return 
	 */
	public boolean paramAnalyzing(){
		//set the vina path in local path
		String vinaPath = "/usr/local/cloud/vina/autodock_vina_1_1_2_linux_x86/bin/vina";
		//make the whole vina command
		vina_command = vinaPath + " --config " + config_file +" --ligand " + ligand_file + " --seed " + seed +
				        " --receptor " + receptor_file + " --out " + output_file + " --log " + output_log;
		//if ligand file's source path is null. default set it ligand_file path; this is for exception log;
		if (ligand_file_source_path == null){
			ligand_file_source_path = ligand_file;
		}
		
		//get the log's dir, if not exist ,will create it ;
		File logF = new File(output_log);
		System.out.println("logdir is: " +logF.getParent());
		File logdir = new File(logF.getParent());
		
		if (!logdir.exists()){
			if(!logdir.mkdirs()){
				System.out.println("Create log directory fail.");
				exception_content = "Create log directory fail.";
				return false;
			}
		}
	    
		//check if arguments are legal
		if ((config_file == null) || (!ligand_file.endsWith(".pdbqt")) || (!receptor_file.endsWith(".pdbqt")) 
			|| (!output_file.endsWith(".pdbqt")) || (output_log == null) || (seed <= 0)){
			System.out.println("Usage: java -jar RunVina.jar [config_file] [ligand_file] [receptor_file] [seed] [output_file] [output_log] [ligand_file_source_path]");
			exception_content = "Vina Arguments wrong.";
			return false;
		}
		//check whether the three input file exist.
		if ((!new File(config_file).isFile()) || (!new File(ligand_file).isFile()) || (!new File(receptor_file).isFile())){
			System.out.println("Input files are not all exist, plz have a check.");
			exception_content = "Three input file are not all exist.";
			return false;
		}

		return true;
	}

	/**
	 * run vina thread and check if thread running well
	 * @return
	 */
	public boolean runVinaThread(){
		
		boolean flag = true;
		System.out.println(vina_command);
        //start a thread for vina with vina command
        VinaThread vinathread = new VinaThread(vina_command);
		vinathread.start();
		try {
			System.out.println("wait vina for 40 minutes...");
			vinathread.join(2400000);
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
			flag = false;
		}
		//check if the thread is finished ,if not ,will kill it with exception;
		while (vinathread.isAlive()){
			System.out.println("vina is still run, it seems like blocked ,need kill it");
			flag = false;
			exception_content = "Exception: Vina running time more then 40 minutes.";
			vinathread.interrupt();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
		return flag;
	}

	/**
	 * check the vina result file ,dose it exist or illegal
	 * @return
	 */
	public boolean checkResultFile(){
		boolean flag = true;
		//check if the result file exist;
		File resultF = new File(output_file);
		if (!resultF.exists()){
			System.out.println("Result file is not exist.");
			exception_content = "Result file is not exist, you can check ligand file and vina command.";
			return false;
		}
		//check if the result file's size is 0 bytes;
		FileInputStream in = null;
		try {
			in = new FileInputStream(resultF);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			if (in.available()==0){
				resultF.delete();
				System.out.println("Result file size is 0 byte.");
				exception_content = "Result file size is 0 byte, which has been deleted.";
				flag = false;
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} finally {
			if(in != null){
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		return flag;
	}

	public String writeException(){
		
		String ip = null;
		SimpleDateFormat stf = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
		String time = stf.format(new Date());
		String exception_info = null;

		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			System.out.println("IP is: " + ip);
		} catch (UnknownHostException e) {
			System.out.println(e.getMessage());
		}
		exception_info = " Source File :" + ligand_file_source_path + "\n\n" +
				         " Run vina IP :" + ip + "\n\n" +
				         " Run    Time :" + time + "\n\n" +
				         " VinaCommand :" + vina_command + "\n\n" +
				         " Except Info :" + exception_content;
		return exception_info;
	}
}
