package com.dinginfo.seamq.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import javax.swing.plaf.basic.BasicScrollBarUI;

import com.dinginfo.seamq.BasicShutdown;
import com.dinginfo.seamq.ServerConfig;

public class MonitorShutdown extends BasicShutdown {
	public MonitorShutdown(String shutdownCmd,int shutdownPort) {
		super(shutdownCmd, shutdownPort);
	}

	public static void main(String[] args) {
		ServerConfig config = new ServerConfig();
		String shutdownCmd = config.getMonitorShutdownCommand();
		int shutdownPort = config.getMonitorShutdownPort();
		MonitorShutdown server = new MonitorShutdown(shutdownCmd,shutdownPort);
		server.shutdown();
	}
}
