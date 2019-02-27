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

package com.dinginfo.seamq.broker;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.ApplicationDaemon;
import com.dinginfo.seamq.MultiMap;
import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.ShutdownListener;
import com.dinginfo.seamq.ZKUtils;
import com.dinginfo.seamq.common.KetamaNodeLocator;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.NodeInfo;
import com.dinginfo.seamq.scheduler.RegisterZkNode;
import com.dinginfo.seamq.service.NotifyService;
import com.dinginfo.seamq.service.QueueService;
import com.dinginfo.seamq.service.TopicService;

public class BrokerServer extends ApplicationDaemon {
	private final static Logger logger = LogManager.getLogger(BrokerServer.class);
	
	public static final String VERSION="1";
	
	private RegisterZkNode brokerRegister;
	
	private ZkClient zkclient = null;
	
	private ExecutorService executorService;
	
	private LinkedBlockingQueue taskQueue;
	
	private String currentNodeId ;
	
	private KetamaNodeLocator ketamaNodeLocator ;
	
	private int virtualBrokerCopys =10000;
	
	private List<String> brokerIdList;
	
	private MultiMap<String> brokerIdMap;
	
	private QueueService queueService ;
	
	private NotifyService notifyService;
	
	public BrokerServer(ServerConfig config) {
		super(config);
		this.context.setVersion(VERSION);
		this.taskQueue = new LinkedBlockingQueue();
		int poolSize = config.getTaskThreadPoolSize();
		this.executorService = new ThreadPoolExecutor(poolSize, poolSize, 100, TimeUnit.SECONDS, taskQueue);
		this.messageInitializer = new BrokerMessageInitializer(context,executorService,taskQueue);
		this.brokerIdMap = new MultiMap<String>(config.getBrokerBucketSize());
		this.brokerIdList = new ArrayList<String>();
		this.notifyService = new NotifyService(config);
		this.notifyService.setTopicService(MyBean.getBean(TopicService.BEAN_NAME, TopicService.class));
		context.putBean(NotifyService.BEAN_NAME, notifyService);
	}
	
	public void registerBroker(ServerConfig config)throws Exception{
		 InetAddress address = InetAddress.getLocalHost();
		 String ip =address.getHostAddress();
		 String host = address.getHostName();
		 int port = config.getBrokerPort();
		 NodeInfo node = new NodeInfo();
		 node.setHost(host);
		 node.setIp(ip);
		 node.setPort(port);
		 StringBuilder sb = new StringBuilder();
		 sb.append(host);
		 sb.append(":");
		 sb.append(port);
		 node.setId(StringUtil.generateMD5String(sb.toString()));
		 currentNodeId = node.getId();
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
		 
		 sb.append("/brokers");
		 String brokers = sb.toString();
		 if(!zkclient.exists(brokers)){
			 zkclient.createPersistent(brokers);
		 }
		 int brokerBucketSize = config.getBrokerBucketSize();
		 String brokerPath = sb.toString();
		 String bucketPath = null;
		 for(int i=0;i<brokerBucketSize;i++){
			 sb = new StringBuilder(100);
			 sb.append(brokerPath);
			 sb.append("/");
			 sb.append(String.valueOf(i));
			 bucketPath = sb.toString();
			 if(!zkclient.exists(bucketPath)){
				 zkclient.createPersistent(bucketPath);
			 }
		 }
		 sb = new StringBuilder(100);
		 sb.append(brokerPath);
		 sb.append("/");
		 
		 int k = node.getId().hashCode() % brokerBucketSize;
		 if(k<0){
			 k = Math.abs(k);
		 }
		 sb.append(String.valueOf(k));
		 sb.append("/");
		 sb.append(node.getId());
		 
		 brokerPath = sb.toString();
		 
		 boolean registed=false; 
		 long sleepTime = 1000 * 10;
		 int sleepCount =0;
		 while(true){
			 registed =zkclient.exists(brokerPath);
			 if(!registed){
				 zkclient.createEphemeral(brokerPath, jsonString);
				 break;
			 }
			 Thread.sleep(sleepTime);
			 sleepCount +=1;
			 if(sleepCount>=6){
				 sleepCount =0;
				 logger.info("register broker,sleep 60 second");	 
			 }
		 }
		 
		 brokerRegister = new RegisterZkNode(zkclient, brokerPath, jsonString);
		 scheduledService = Executors.newScheduledThreadPool(1);
		 scheduledService.scheduleAtFixedRate(brokerRegister, 60, 30, TimeUnit.SECONDS);
	}

	public void connectZookeeper(String zookeeperUrl)throws Exception{
		zkclient = new ZkClient(zookeeperUrl);
	}
	
	public void subscribeBrokers()throws Exception{
		StringBuilder sb = new StringBuilder(100);
		sb.append("/");
		sb.append(config.getMQRoot());
		sb.append("/brokers/");
		String brokerPath = sb.toString();
		String bucketPath = null;
		int brokerBucketSize = config.getBrokerBucketSize();
		for (int i = 0; i < brokerBucketSize; i++) {
			sb = new StringBuilder(100);
			sb.append(brokerPath);
			sb.append(String.valueOf(i));
			bucketPath = sb.toString();
			zkclient.subscribeChildChanges(bucketPath, new IZkChildListener() {

				@Override
				public void handleChildChange(String path, List<String> nodeList)
						throws Exception {
					String[] nameArray = path.split("/");
					String bucketName = nameArray[nameArray.length - 1];
					Map<String, String> objMap = brokerIdMap.getMap(bucketName);
					if (objMap == null) {
						return;
					}
					objMap.clear();
					for (String nodeName : nodeList) {
						objMap.put(nodeName, nodeName);
					}
					brokerIdList = brokerIdMap.getObjectList();
					ketamaNodeLocator.rebuildKetamaNodeMap(brokerIdList);
					queueService.rebuildQueue();
				}
			});
		}
	
	}
	
	public void init() {
		try {
			zkclient = new ZkClient(config.getZookeeperURLs());
			List<NodeInfo> brokerNodeInfoList = getBrokerNodeInfos();
			if(brokerNodeInfoList!=null && brokerNodeInfoList.size()>0){
				for(NodeInfo nodeInfo : brokerNodeInfoList){
					brokerIdMap.put(nodeInfo.getId(), nodeInfo.getId());
				}
			}
			brokerIdList= brokerIdMap.getObjectList();
			ketamaNodeLocator =new KetamaNodeLocator(brokerIdList,virtualBrokerCopys);
			
		} catch (Exception e) {
			logger.error(e.fillInStackTrace());
		}
		
	}
	
	private List<NodeInfo> getBrokerNodeInfos()throws Exception{
		return ZKUtils.getBrokerNodeInfos(zkclient,config.getMQRoot(),config.getBrokerBucketSize());
	}
	
	public void loadQueueService()throws Exception{
		QueueService service = MyBean.getBean(QueueService.BEAN_NAME, QueueService.class);
		service.setCurrentNodeId(currentNodeId);
		service.setKetamaNodeLocator(ketamaNodeLocator);
		this.queueService = service;
	}
	
	@Override
	public void shutdown() { 
		super.shutdown();
		if(executorService!=null){
			executorService.shutdown();	
		}
		if(zkclient!=null){
			zkclient.close();
		}
		if(notifyService!=null) {
			notifyService.shutdown();
		}
		
	}

	public static void main(String[] args) {
		MyBean.start();
		ServerConfig config = new ServerConfig();
		config.setMaster(false);
		final BrokerServer server = new BrokerServer(config);
		try {
			server.connectZookeeper(config.getZookeeperURLs());
			server.registerBroker(config);
			server.init();
			server.loadQueueService();
			server.subscribeBrokers();
			logger.info("starting broker service");
			server.start(config.getBrokerPort());
			
			//shutdown listen server
			String shutdown = config.getBrokerShutdownCommand();
			int port = config.getBrokerShutdownPort();
			ShutdownListener stopListener = new ShutdownListener(server, shutdown, port);
			stopListener.start();
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					server.shutdown();
				}
			}));
			logger.info("start broker service,port:"+config.getBrokerPort());
			
		} catch (Exception e) {
			logger.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
	}

}
