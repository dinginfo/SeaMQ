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

package com.dinginfo.seamq.master;


import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.ApplicationDaemon;
import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.ShutdownListener;
import com.dinginfo.seamq.client.MQClientService;
import com.dinginfo.seamq.client.MQConfig;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.NodeInfo;
import com.dinginfo.seamq.scheduler.RegisterZkNode;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.service.TopicService;
import com.dinginfo.seamq.service.UserService;

public class MasterServer extends ApplicationDaemon {
	private final static Logger logger = LogManager.getLogger(MasterServer.class);
	
	private String masterId=null;
	
	private RegisterZkNode masterRegister;
	
	private ZkClient zkclient;
	
	private MQClientService clientService;
	
	public MasterServer(ServerConfig config){
		super(config);
		MQConfig clientConfig = new MQConfig();
		clientConfig.setMQRoot(config.getMQRoot());
		clientConfig.setZookeeperURL(config.getZookeeperURLs());
		this.clientService = new MQClientService(clientConfig);
		this.clientService.init();
		context.putBean(MQClientService.BEAN_NAME, clientService);
		this.messageInitializer = new MasterMessageInitializer(context);
		this.scheduledService = Executors.newScheduledThreadPool(1);
		  
		
	}
	public void init()throws Exception{
		this.clientService.subscribeBrokers();
		zkclient = clientService.getZkclient();		
	}
	
	public void registerMaster(ServerConfig config)throws Exception{
		 InetAddress address = InetAddress.getLocalHost();
		 String ip =address.getHostAddress();
		 String hostName = address.getHostName();
		 int port = config.getMasterPort();
		 NodeInfo node = new NodeInfo();
		 node.setHost(hostName);
		 node.setIp(ip);
		 node.setPort(port);
		 StringBuilder sb = new StringBuilder();
		 sb.append(hostName);
		 sb.append(":");
		 sb.append(port);
		 node.setId(StringUtil.generateMD5String(sb.toString()));
		 masterId = node.getId();
		 String jsonString = JSON.toJSONString(node);
		 
		 String zkurl = config.getZookeeperURLs();
		 String zkRoot =config.getMQRoot();
		 sb = new StringBuilder();
		 if(!"/".equals(zkRoot.substring(0, 1))){
			 sb.append("/");	 
		 }
		 sb.append(zkRoot);
		 
		 zkRoot = sb.toString();
		 if(!zkclient.exists(zkRoot)){
			 zkclient.createPersistent(zkRoot);
		 }
		 
		 sb.append("/master");
		 String masterPath=sb.toString();
		 boolean registed=false; 
		 long sleepTime = 1000 * 10;
		 int sleepCount =0;
		 NodeInfo masterNode =null;
		 String masterJson =null;
		 while(true){
			 registed =zkclient.exists(masterPath);
			 if(!registed){
				 zkclient.createEphemeral(masterPath, jsonString);
				 masterJson =zkclient.readData(masterPath);
				 if(masterJson!=null){
					 masterNode = JSON.parseObject(masterJson, NodeInfo.class);
					 if(masterNode!=null && masterId.equals(masterNode.getId())){
						 break;	 
					 }
				 }
				 
			 }
			 Thread.sleep(sleepTime);
			 sleepCount +=1;
			 if(sleepCount>=6){
				 sleepCount =0;
				 logger.info("register Master,sleep 60 second");	 
			 }
		 }
		 masterRegister = new RegisterZkNode(zkclient, masterPath, masterJson);
		 
		 scheduledService.scheduleAtFixedRate(masterRegister, 60, 10, TimeUnit.SECONDS);
	}
	
	private void loadUser()throws Exception{
		UserService service = MyBean.getBean(UserService.BEAN_NAME, UserService.class);
		if(service!=null) {
			service.load();	
		}
		
	}
	
	private void loadSession() throws Exception{
		SessionService service = MyBean.getBean(SessionService.BEAN_NAME, SessionService.class);
		if(service!=null) {
			service.load();	
		}
	}
	
	private void loadTopic()throws Exception{
		TopicService service = MyBean.getBean(TopicService.BEAN_NAME, TopicService.class);
		if(service!=null) {
			service.load();	
		}
	}
	
	
	@Override
	public void shutdown() {
		super.shutdown();
		if(clientService!=null){
			clientService.shutdown();	
		}
	}
	public static void main(String[] args) {
		MyBean.start();
		ServerConfig config = new ServerConfig();
		config.setMaster(true);
		final MasterServer server = new MasterServer(config);
		
		try {
			server.init();
			server.registerMaster(config);
			server.loadUser();
			server.loadSession();
			server.loadTopic();
			logger.info("starting master service");
			server.start(config.getMasterPort());
			
			//shutdown listen server
			String shutdown = config.getMasterShutdownCommand();
			int port = config.getMasterShutdownPort();
			ShutdownListener stopListener = new ShutdownListener(server, shutdown, port);
			stopListener.start();
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					server.shutdown();
				}
			}));
			logger.info("start master service,port:"+config.getMasterPort());
		} catch (Exception e) {
			logger.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
	}

}
