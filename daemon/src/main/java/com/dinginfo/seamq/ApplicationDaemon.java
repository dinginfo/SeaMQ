/*
 * Copyright 2016 David Ding.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.dinginfo.seamq;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ApplicationDaemon {
	private final static Logger logger = LogManager.getLogger(ApplicationDaemon.class);

	private InetSocketAddress address;

	private ServerBootstrap bootstrap;
	private EventLoopGroup bossGroup = null;
	private EventLoopGroup workerGroup = null;
	private ChannelFuture cf = null;
	private Map<String, String> seckeyMap = null;
	protected ServerConfig config;
	protected ServiceContext context =null;
	protected ScheduledExecutorService scheduledService ;
	protected ChannelInitializer messageInitializer;

	public ApplicationDaemon(ServerConfig config) {
		this.config = config;
		this.context = new ServiceContext();
		this.context.setConfig(config);
		this.context.setNotifyMessage(config.isNotifyMessage());
	}

	public void start(int port) {


		seckeyMap = config.getSeckeyMap();
		int bossThreadSize = config.getBossThreadPoolSize();
		if(bossThreadSize>0){
			bossGroup = new NioEventLoopGroup(bossThreadSize);	
		}else{
			bossGroup = new NioEventLoopGroup();
		}
		
		int workThreadSize = config.getWorkThreadPoolSize();
		if(workThreadSize>0){
			workerGroup = new NioEventLoopGroup(workThreadSize);	
		}else{
			workerGroup = new NioEventLoopGroup();
		}
		
		try {
			bootstrap = new ServerBootstrap();
			
			bootstrap
					.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class)
					.childHandler(messageInitializer);
			cf = bootstrap.bind(port).sync();
			String info = "Server start on "
					+ String.valueOf(port);
			System.out.println(info);
			logger.info(info);
		} catch (Exception e) {
			logger.info(e.getMessage());
			e.printStackTrace();
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}

	}

	public void shutdown() {
		try {

			if(cf!=null){
				cf.channel().close();	
			}
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			if(scheduledService!=null){
				scheduledService.shutdown();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public void setAddress(InetSocketAddress address) {
		this.address = address;
	}
	
	

}
