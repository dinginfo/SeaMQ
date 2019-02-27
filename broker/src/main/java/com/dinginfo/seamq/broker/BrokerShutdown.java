package com.dinginfo.seamq.broker;

import com.dinginfo.seamq.BasicShutdown;
import com.dinginfo.seamq.ServerConfig;

public class BrokerShutdown extends BasicShutdown {
	
	public BrokerShutdown(String shutdownCmd ,int port) {
		super(shutdownCmd, port);
	}

	public static void main(String[] args) {
		ServerConfig config = new ServerConfig();
		String shutdownCmd = config.getBrokerShutdownCommand();
		int shutdownPort = config.getBrokerShutdownPort();
		BrokerShutdown server = new BrokerShutdown(shutdownCmd,shutdownPort);
		server.shutdown();
	}
}
