package com.dinginfo.seamq.client;

import java.util.Date;

import com.dinginfo.seamq.entity.MQSession;

public class HttpMQProducer extends MQProducer {
	
	protected HttpMQProducer(MQClientService service,long domainId,String sessionId){
		super(service);
		this.domainId = domainId;
		session = new MQSession();
		session.setId(sessionId);
		Date date = new Date(System.currentTimeMillis());
		session.setCreatedTime(date);
		session.setUpdatedTime(date);
	}

}
