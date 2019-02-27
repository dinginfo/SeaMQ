package com.dinginfo.seamq.scheduler;

import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.broker.BrokerMessageServerHandler;

public class CommondHandlerTask implements Runnable {
	private MessageRequest request;

	private MessageResponse response;
	
	private BrokerMessageServerHandler messageHandler;
	
	public CommondHandlerTask(MessageRequest request, MessageResponse response,BrokerMessageServerHandler handler){
		this.request = request;
		this.response = response;
		this.messageHandler = handler;
	}
	
	@Override
	public void run() {
		messageHandler.doHandle0(request, response);
	}

}
