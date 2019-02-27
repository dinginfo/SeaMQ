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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.MultiMap;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ZKUtils;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.TopicCommand;
import com.dinginfo.seamq.common.KetamaNodeLocator;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.NodeInfo;
import com.dinginfo.seamq.entity.mapping.TopicMapping;

public class MQClientService {
	public static final String BEAN_NAME = "clientService";
	
	private final static Logger logger = LogManager.getLogger(MQClientService.class);
	
	private MessageClientContext context;
	
	private MQConfig config;
	
	private long objectTimeout = 2 * 60 * 1000;
	
	private CachedMap<String,MessageClient> clientMap;
	
	private CachedMap<String,NodeInfo> brokerNodeInfoMap;
	
	private CachedMap<String, MQTopic> topicMap;
	
	private CachedMap<String,MQueue> queueMap;
	
	private List<String> brokerIdList;
	
	private MultiMap<String> brokerIdMap;
	
	private ZkClient zkclient;
	
	private Map<String, Command> commandMap;
	
	private int connectionRepeatNum = 100;
	
	private int messageRepeatNum = 3;
	
	private KetamaNodeLocator ketamaNodeLocator;
	
	private int virtualBrokerCopys =10000;
	
	private MessageClient masterClient ;
	
	public MQClientService(MQConfig config){
		this.config = config;
		
		if(config.getMQObjectTimeout()>0){
			objectTimeout = config.getMQObjectTimeout();	
		}
		if(config.getConnectionRepeatNum()>0){
			connectionRepeatNum = config.getConnectionRepeatNum();
		}
		if(config.getMessageRepeatNum()>0){
			messageRepeatNum = config.getMessageRepeatNum();
		}
		topicMap = new CachedMap<String,MQTopic>(50000);
		queueMap = new CachedMap<String,MQueue>(50000);
		clientMap = new CachedMap<String,MessageClient>(20000);
		brokerNodeInfoMap = new CachedMap<String,NodeInfo>(20000);
		brokerIdMap = new MultiMap<String>(config.getBrokerBucketSize());
		brokerIdList = new ArrayList<String>();
		context = new MessageClientContext();
		context.setOperateTimeout(config.getOperateTimeout());
		commandMap = new HashMap<String, Command>();
		context.setCommandMap(commandMap);
	}
	
	public void init() {
		try {
			zkclient = new ZkClient(config.getZookeeperURL());
			List<NodeInfo> brokerNodeInfoList = getBrokerNodeInfos();
			if(brokerNodeInfoList!=null && brokerNodeInfoList.size()>0){
				for(NodeInfo nodeInfo : brokerNodeInfoList){
					brokerIdMap.put(nodeInfo.getId(), nodeInfo.getId());
					brokerNodeInfoMap.put(nodeInfo.getId(), nodeInfo);
				}
			}
			brokerIdList= brokerIdMap.getObjectList();
			ketamaNodeLocator =new KetamaNodeLocator(brokerIdList,virtualBrokerCopys);
			
		} catch (Exception e) {
			logger.error(e.fillInStackTrace());
		}
		
	}
	
	public String getQueueLocation(String queueId){
		if(ketamaNodeLocator==null){
			return null;
		}
		String nodeId= ketamaNodeLocator.getNodeInfo(queueId);
		return nodeId;	
	} 
	
	public void subscribeMaster()throws Exception{
		StringBuilder sb =new StringBuilder(100);
		sb.append("/");
		sb.append(config.getMQRoot());
		sb.append("/master");
		String path =sb.toString();
		zkclient.subscribeDataChanges(path, new IZkDataListener() {
			
			@Override
			public void handleDataDeleted(String path) throws Exception {
				
				if(masterClient!=null){
					masterClient.shutdown();
					masterClient = null;
				}
			}
			
			@Override
			public void handleDataChange(String path, Object data) throws Exception {
				try{
					getMasterClient();	
				}catch(Exception e){
					logger.error(e.getMessage());
				}
			}
		});
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
				}
			});
		}
	
	}
	

	public List<NodeInfo> getBrokerNodeInfos()throws Exception{
		return ZKUtils.getBrokerNodeInfos(zkclient,config.getMQRoot(),config.getBrokerBucketSize());
	}
	
	public MessageClient getMasterClient() throws Exception {
		if(masterClient!=null){
			return masterClient;
		} 
		
		NodeInfo masterInfo = ZKUtils.getMasterNodeInfo(zkclient, config.getMQRoot());
		if(masterInfo==null){
			return null;
		}
		
		InetSocketAddress socketAddress  = null;
		if(masterInfo.getHost()!=null && masterInfo.getHost().trim().length()>0){
			socketAddress = new InetSocketAddress(masterInfo.getHost().trim(), masterInfo.getPort()); 
		}else if (masterInfo.getIp()!=null && masterInfo.getIp().trim().length()>0){
			socketAddress = new InetSocketAddress(masterInfo.getIp().trim(), masterInfo.getPort());
		}
		
		masterClient = new MessageClient(context, socketAddress);
		return masterClient;
	}
	
	public MessageClient getMessageClient(String nodeId)throws Exception{
		MessageClient client=clientMap.get(nodeId);
		if(client!=null){
			if(!client.isContected()){
				client.contect();
			}
			return client;
		}
		
		NodeInfo nodeInfo = brokerNodeInfoMap.get(nodeId);
		if(nodeInfo==null){
			String zkRoot = config.getMQRoot();
			int brokerBucketSize = config.getBrokerBucketSize();
			nodeInfo = ZKUtils.getBrokerNodeInfo(zkclient, zkRoot, brokerBucketSize, nodeId);
		}
		if(nodeInfo==null){
			return null;
		}
		brokerNodeInfoMap.put(nodeId, nodeInfo);
		InetSocketAddress socketAddress  = null;
		if(nodeInfo.getHost()!=null && nodeInfo.getHost().trim().length()>0){
			socketAddress = new InetSocketAddress(nodeInfo.getHost().trim(), nodeInfo.getPort()); 
		}else if (nodeInfo.getIp()!=null && nodeInfo.getIp().trim().length()>0){
			socketAddress = new InetSocketAddress(nodeInfo.getIp().trim(), nodeInfo.getPort());
		}
				
		client = new MessageClient(context, socketAddress);
		clientMap.put(nodeInfo.getId(), client);
		return client;
	}
	
	public MQTopic getTopicByName(long domainId,String topicName)throws Exception{
		MQTopic topic = new MQTopic();
		topic.setDomain(new MQDomain(domainId));
		topic.setName(topicName);
		String topicId = topic.buildTopicId();
		return getTopicInfo(domainId, topicId);
	}
	
	public MQTopic getTopicInfo(long domainId,String topicId) throws Exception {
		MQTopic topic  = getTopic(topicId);
		if(topic!=null){
			return topic;
		}
		MessageClient client =getBroker();
		if(client==null){
			throw new Exception("no broker");
		}
		topic  =new MQTopic();
		topic.setDomain(new MQDomain(domainId));
		topic.setId(topicId);
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GET,topic);
		MQResponse response =client.writeMessage(command);
		
		if(response == null){
			return null;
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			String name = response.getStringAttribute(TopicMapping.FIELD_NAME);
			if (name == null) {
				return null;
			}
			topic = new MQTopic(domainId,name);
			topic.buildTopicId();
			topic.setQueueNum(response.getIntAttribute(TopicMapping.FIELD_QUEUE_NUM));
			topic.setStatus(response.getStringAttribute(TopicMapping.FIELD_STSTUS));
			putTopic(topic);
			return topic;
		}
		String exceptionString = response.getException();
		if(exceptionString!=null && exceptionString.trim().length()>0){
			throw new Exception(response.getException());
		}
		
		return null;
	}
	
	private void putTopic(MQTopic topic){
		if(topic==null){
			return;
		}
		Date date = new Date(System.currentTimeMillis());
		topic.setCreatedTime(date);
		topicMap.put(topic.getId(), topic);
	}
	
	private MQTopic getTopic(String topicId){
		if(topicId==null) {
			return null;
		}
		MQTopic topic = topicMap.get(topicId);
		if(topic==null){
			return null;
		}
		
		Date createdTime = topic.getCreatedTime();
		long time = System.currentTimeMillis()-createdTime.getTime();
		if(time<objectTimeout){
			return topic;	
		}else{
			topicMap.remove(topicId);
			return null;
		}
	}
	
	public MessageClient getBroker()throws Exception{
		if(brokerIdList==null || brokerIdList.size()==0){
			return null;
		}
		MessageClient client =null;
		if(brokerIdList.size()==1){
			client = getMessageClient(brokerIdList.get(0));
		}else{
			Random r = new Random();
			int index =r.nextInt(brokerIdList.size());
			if(index<0){
				index = Math.abs(index);
			}
			client = getMessageClient(brokerIdList.get(index));
		}
		return client;
	}
	
	public NodeInfo getNodeInfo(String nodeId)throws Exception{
		NodeInfo ninfo = brokerNodeInfoMap.get(nodeId);
		if(ninfo!=null){
			return ninfo;
		}
		String id = brokerIdMap.get(nodeId);
		if(id==null){
			return null;
		}
		String zkRoot = config.getMQRoot();
		int brokerBucketSize = config.getBrokerBucketSize();
		ninfo = ZKUtils.getBrokerNodeInfo(zkclient, zkRoot, brokerBucketSize, nodeId);
		if(ninfo!=null){
			brokerNodeInfoMap.put(nodeId, ninfo);
		}
		return ninfo;
	}
	
	private MQueue getQueue(String queueId){
		MQueue queue =queueMap.get(queueId);
		if(queue==null){
			return null;
		}
		Date createdTime = queue.getCreatedTime();
		long time = System.currentTimeMillis()-createdTime.getTime();
		
		if(time<objectTimeout){
			return queue;
		}else{
			queueMap.remove(queueId);
			return null;
		}
	}
	
	private void putQueue(MQueue queue){
		if(queue==null){
			return ;
		}
		Date date = new Date(System.currentTimeMillis());
		queue.setCreatedTime(date);
		queueMap.put(queue.getId(), queue);
	}
	
	public boolean containsBrokerId(String brokerId){
		return brokerIdMap.containsKey(brokerId);
	}
	
	
	public MessageClient getMessageClientByQueueId(String queueId)throws Exception{
		String nodeId = getQueueLocation(queueId);
		if(nodeId==null){
			return null;
		}
		return  getMessageClient(nodeId);
	}
	
	public MQConfig getMQConfig(){
		return this.config;
	}
	
	public ZkClient getZkclient() {
		return zkclient;
	}

	public int getConnectionRepeatNum() {
		return connectionRepeatNum;
	}

	public void setConnectionRepeatNum(int connectionRepeatNum) {
		this.connectionRepeatNum = connectionRepeatNum;
	}

	public int getMessageRepeatNum() {
		return messageRepeatNum;
	}

	public void setMessageRepeatNum(int messageRepeatNum) {
		this.messageRepeatNum = messageRepeatNum;
	}

	public void setMessageNotifyHandler(MessageNotifyHandler notifyHandler){
		this.context.setMessageNotifyHandler(notifyHandler);
	}
	
	public void shutdown() {
		if(zkclient!=null){
			zkclient.close();
		}
		if(masterClient!=null){
			masterClient.shutdown();
		}
		Map<String,MessageClient> map = clientMap.getAll();
		for(MessageClient msgClient : map.values()){
			msgClient.shutdown();
		}
		
	}

	private Executor buildExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, int poolSize) {
		Executor e = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
				keepAliveTime, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(poolSize));
		if (e instanceof ThreadPoolExecutor) {
			((ThreadPoolExecutor) e)
					.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		}
		return e;
	}
}
