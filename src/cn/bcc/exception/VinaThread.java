package cn.bcc.exception;

import java.io.IOException;

public class VinaThread extends Thread{

	private String vinaCommand;
	private Process vinaPro;
	
	public VinaThread(String vinaCommand){
		this.vinaCommand = vinaCommand;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
			try {
				vinaPro = Runtime.getRuntime().exec(vinaCommand);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.out.println(e1.getMessage());
			}
			try {
				vinaPro.waitFor();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				System.out.println(e1.getMessage());
			}
			try {
				vinaPro.getOutputStream().close();
				vinaPro.getInputStream().close();
				vinaPro.getErrorStream().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
			}
		}
}
