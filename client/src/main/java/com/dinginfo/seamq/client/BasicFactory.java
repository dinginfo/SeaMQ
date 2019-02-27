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

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.entity.MQDomain;

public abstract class BasicFactory {
	private static final int  MAX_ITEM_SIZE= 10000;
	
	private static final int MAX_CONNECT_NUM = 3;
	
	private static final int SLEEP_TIME = 10000;
	
	private  MQClientService clientService = null;
	
	private  CachedMap<String,MQProducer> producerMap= null;
	
	private  CachedMap<String,MQConsumer> consumerMap = null;
	
	
	public void init(){
		producerMap= new CachedMap<>(MAX_ITEM_SIZE);
		consumerMap = new CachedMap<>(MAX_ITEM_SIZE);
	}
	
	protected MQClientService getClientService(){
		return clientService;
	}
	
	public MQProducer createMQProducer(String user,String pwd,MQConfig config)throws Exception{
		if(user==null){
			throw new Exception("user is null");
		}
		
		if(clientService==null){
			createClientService(config);
		}
		MQProducer producer = new MQProducer(clientService);
		String sid = null;
		for(int i=0;i<MAX_CONNECT_NUM;i++) {
			sid=producer.login(MQDomain.DEFAULT_DOMAIN_ID,user, pwd, clientService.getBroker());
			if(sid!=null && sid.trim().length()>0) {
				producerMap.put(sid, producer);
				return producer;
			}else {
				Thread.sleep(SLEEP_TIME);
			}
		}
		return null;
	}

	
	public MQProducer getMQProducer(String sessionId){
		if(sessionId==null){
			return null;
		}
		MQProducer producer = producerMap.get(sessionId);
		return producer;
	}
	
	public void removeMQProducer(String sessionId){
		if(sessionId==null){
			return ;
		}
		producerMap.remove(sessionId);
	}
	
	public MQConsumer createMQConsumer(String user,String pwd,MQConfig config)throws Exception{
		if(user==null){
			throw new Exception("user is null");
		}
		
		if(clientService==null){
			createClientService(config);
		}
		MQConsumer consumer = new MQConsumer(clientService);
		String sid = null;
		for(int i=0;i<MAX_CONNECT_NUM;i++) {
			sid = consumer.login(MQDomain.DEFAULT_DOMAIN_ID,user, pwd, clientService.getBroker());
			if(sid!=null && sid.trim().length()>0) {
				consumerMap.put(sid, consumer);
				return consumer;	
			}else {
				Thread.sleep(SLEEP_TIME);
			}
		}
		return null;
	}

	public  MQConsumer getMQConsumer(String sessionId){
		if(sessionId==null){
			return null;
		}
		MQConsumer consumer  = consumerMap.get(sessionId);
		return consumer;
	}
	
	public  void removeMQConsumer(String sessionId){
		if(sessionId==null){
			return ;
		}
		consumerMap.remove(sessionId);
	}
	
	public void shutdown(){
		if(clientService!=null){
			clientService.shutdown();
		}
	}
	
	protected void putMQProducer(String sessionId,MQProducer producer){
		producerMap.put(sessionId, producer);
	}
	
	protected void putMQConsumer(String sessionId,MQConsumer consumer){
		consumerMap.put(sessionId, consumer);
	}
	
	protected synchronized void createClientService(MQConfig config) throws Exception{
		if(clientService!=null){
			return;
		}
		clientService = new MQClientService(config);
		clientService.init();
		clientService.subscribeBrokers();
	}
}
