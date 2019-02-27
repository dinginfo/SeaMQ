package com.dinginfo.seamq.client;

import java.io.Serializable;

public class MessageNotifyContext implements Serializable {
	private String topicName;
	
	private int queue;
	
	private String sessionId;

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public int getQueue() {
		return queue;
	}

	public void setQueue(int queue) {
		this.queue = queue;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

}
