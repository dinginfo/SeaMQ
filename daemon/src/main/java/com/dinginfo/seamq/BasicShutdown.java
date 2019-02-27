package com.dinginfo.seamq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class BasicShutdown {
	private static Logger logger = LogManager.getLogger(BasicShutdown.class);
	
	private String shutdownCmd;
	
	private int port;

	public BasicShutdown(String shutdown,int port) {
		this.shutdownCmd = shutdown;
		this.port =port;
	}

	public void shutdown() {
		Socket socket = null;
		BufferedReader in;
		PrintWriter out;
		try {
			socket = new Socket("127.0.0.1", port);
			out = new PrintWriter(socket.getOutputStream(), true);
			out.println(shutdownCmd);
			out.close();

		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				if (socket != null)
					socket.close();
			} catch (Exception ee) {

			}
		}
	}
}
