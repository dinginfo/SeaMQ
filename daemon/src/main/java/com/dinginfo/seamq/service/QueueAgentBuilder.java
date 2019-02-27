package com.dinginfo.seamq.service;

import com.dinginfo.seamq.MQueueAgent;

public interface QueueAgentBuilder {
	public static final String BEAN_NAME="queueAgentBuilder";
	
	public MQueueAgent build();

}
