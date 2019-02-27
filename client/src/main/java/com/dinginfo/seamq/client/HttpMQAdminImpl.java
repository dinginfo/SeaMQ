package com.dinginfo.seamq.client;

import java.util.Date;

import com.dinginfo.seamq.entity.MQSession;

public class HttpMQAdminImpl extends MQAdminImpl {

	protected HttpMQAdminImpl(MQClientService clientService,long domainId,String sessionId){
		super(clientService);
		this.domainId = domainId;
		session = new MQSession();
		session.setId(sessionId);
		Date date = new Date(System.currentTimeMillis());
		session.setCreatedTime(date);
		session.setUpdatedTime(date);
	}

}
