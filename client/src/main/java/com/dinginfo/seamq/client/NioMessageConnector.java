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

package com.dinginfo.seamq.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.Command;


/** 
 * @author David Ding
 *
 */
public class NioMessageConnector implements MessageConnector{
	private final static Logger logger = LogManager.getLogger(NioMessageConnector.class);
	private static final long INTERVAL_TIME = 1000;
	private InetSocketAddress address;
	private Channel channel=null;
	private Bootstrap bootstrap=null;
	private boolean connected;
	private EventLoopGroup group = null;
	private String id;
	private long connectedTime;
	private MessageClientContext clientContext;
	
	
	public NioMessageConnector(MessageClientContext context, InetSocketAddress address){
		this.address=address;
		this.clientContext = context;
		init();
	}
	
	public NioMessageConnector(String id,MessageClientContext context, InetSocketAddress address){
		this.id=id;
		this.address=address;
		this.clientContext = context;
		init();
	}
	
	public void init(){
		group = new NioEventLoopGroup();
		bootstrap = new Bootstrap();
		bootstrap.group(group)
		.channel(NioSocketChannel.class)
		.handler(new MessageClientInitializer(clientContext));
		connect();
	}
	
	public synchronized boolean connect(){
		if(connected){
			return true;
		}
		try {
			if((System.currentTimeMillis()-connectedTime)<INTERVAL_TIME){
				return false;
			}
			channel=bootstrap.connect(this.address).sync().channel();
			if(channel!=null){
				connected=true;
			}else{
				connected=false;
			}
			
		} catch (Exception e) {
			connected = false;
			logger.info(e.getMessage());
		}finally{
			connectedTime = System.currentTimeMillis();
		}
		return connected;
		
	}
	
	public synchronized void writeMessage(Object msg){
		//ChannelFuture future= channel.write(msg);
		//System.out.println(msg);
		ChannelFuture future= channel.writeAndFlush(msg);
		
		future.addListener(new ChannelFutureListener() {
			
		    void reconnect(){
		    	connected=false;
		    	connectedTime = System.currentTimeMillis();
				logger.info("Write message is not success,reconnect session server,"+NioMessageConnector.this.toString());
		    }
			
			@Override
			public void operationComplete(ChannelFuture channelFuture) throws Exception {
				if(!channelFuture.isSuccess()){
					if(channelFuture.cause() instanceof ClosedChannelException){
						reconnect();
					}else if (channelFuture.cause() instanceof java.net.ConnectException){
						reconnect();
					}
				}
			}
		});
	}
	
	public boolean checkConnected(){
		if(!connected){
			connect();
		}
		return connected;
	}
	
	
	public void shutdown(){
		if(channel!=null){
			 channel.disconnect();
			 try {
				channel.closeFuture().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
		 }
		 if(group!=null){
   		   try {
				group.shutdownGracefully().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}   
   	  }   
	}

	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("IP:");
		sb.append(address.getHostName());
		sb.append(",Port:");
		sb.append(address.getPort());
		return sb.toString();
	}

	public String getId() {
		return id;
	}	
}
