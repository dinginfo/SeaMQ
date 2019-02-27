package com.dinginfo.seamq.client;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.entity.MQDomain;

public class HttpMQFactory extends BasicFactory{
	private static HttpMQFactory instance;

	public static HttpMQFactory getInstance(){
		if(instance==null){
			createHttpMQFactory();
		}
		return instance;
	} 
	
	public MQProducer createMQProducer(String sessionId,MQConfig config) throws Exception{
		MQClientService clientService = getClientService();
		if(clientService==null){
			createClientService(config);
		}
		long domainId = MQDomain.DEFAULT_DOMAIN_ID;
		HttpMQProducer producer = new HttpMQProducer(clientService, domainId, sessionId);
		putMQProducer(sessionId, producer);
		return producer;
	}
	
	public MQConsumer createMQConsumer(String sessionId,MQConfig config){
		MQClientService clientService = getClientService();
		if(clientService==null){
			return null;
		}
		long domainId = MQDomain.DEFAULT_DOMAIN_ID;
		HttpMQConsumer consumer = new HttpMQConsumer(clientService, domainId, sessionId);
		putMQConsumer(sessionId, consumer);
		return consumer;
	}
	
	private static synchronized void createHttpMQFactory(){
		if(instance!=null){
			return;
		}
		instance = new HttpMQFactory();
	}
	
	private HttpMQFactory(){
		init();
	}
}
