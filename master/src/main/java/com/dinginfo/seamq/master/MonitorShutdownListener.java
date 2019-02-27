package com.dinginfo.seamq.master;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MonitorShutdownListener extends Thread {
	private static final Logger logger = LogManager.getLogger(MonitorShutdownListener.class);
	
	private MonitorServer daemon;
	private String shutdownCmd = null;
	private int shutdownPort;

	public MonitorShutdownListener(){}

	public MonitorShutdownListener(MonitorServer daemon, String shutdownCmd,
			int shutdownPort) {
		this.daemon = daemon;
		this.shutdownCmd = shutdownCmd;
		this.shutdownPort = shutdownPort;
	}

	public void run() {
		doRun();
	}

	private void doRun() {
		ServerSocket server = null;
		try {
			server = new ServerSocket(shutdownPort,0,InetAddress.getByName("127.0.0.1"));
			Socket s=null;
			while(true){
				s = server.accept();
				InputStreamReader isr = new InputStreamReader(s.getInputStream());
				BufferedReader br = new BufferedReader(isr);
				String str = br.readLine();
				if (shutdownCmd.equals(str)) {
					break;
				}	
			}
			s.close();
			daemon.shutdown();
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			closeServerSocket(server);
		}

	}

	private void closeServerSocket(ServerSocket socket) {
		try {
			if (socket != null) {
				socket.close();
			}
		} catch (Exception e) {

		}
	}

	public String getShutdownCmd() {
		return shutdownCmd;
	}

	public void setShutdownCmd(String shutdownCmd) {
		this.shutdownCmd = shutdownCmd;
	}

	public int getShutdownPort() {
		return shutdownPort;
	}

	public void setShutdownPort(int shutdownPort) {
		this.shutdownPort = shutdownPort;
	}
}
