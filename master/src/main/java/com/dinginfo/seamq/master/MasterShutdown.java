package com.dinginfo.seamq.master;

import com.dinginfo.seamq.BasicShutdown;
import com.dinginfo.seamq.ServerConfig;

public class MasterShutdown extends BasicShutdown {
	
	public MasterShutdown(String shutdownCmd ,int port) {
		super(shutdownCmd, port);
	}

	public static void main(String[] args) {
		ServerConfig config = new ServerConfig();
		String shutdownCmd = config.getMasterShutdownCommand();
		int shutdownPort = config.getMasterShutdownPort();
		MasterShutdown server = new MasterShutdown(shutdownCmd,shutdownPort);
		server.shutdown();
	}
}
