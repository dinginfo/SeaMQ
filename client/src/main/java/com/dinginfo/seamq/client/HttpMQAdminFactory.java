package com.dinginfo.seamq.client;

import com.dinginfo.seamq.entity.MQDomain;

public class HttpMQAdminFactory extends BasicAdminFactory {
	private static HttpMQAdminFactory instance;

	public static HttpMQAdminFactory getInstance() {
		if (instance == null) {
			createHttpMQFactory();
		}
		return instance;
	}
	
	public MQAdmin createMQAdmin(String sessionId,MQConfig config) throws Exception{
		MQClientService clientService = getClientService();
		if(clientService==null){
			createClientService(config);
		}
		long dominaId= MQDomain.DEFAULT_DOMAIN_ID;
		HttpMQAdminImpl admin = new HttpMQAdminImpl(clientService, dominaId, sessionId);
		putMQAdmin(sessionId, admin);
		return admin;
	}

	private static synchronized void createHttpMQFactory() {
		if (instance != null) {
			return;
		}
		instance = new HttpMQAdminFactory();
	}
	private HttpMQAdminFactory() {
		init();
	}
 
}
