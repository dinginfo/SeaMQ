package com.dinginfo.seamq.client;

import java.io.Serializable;

public class MessageContext implements Serializable {
	private MQConsumer consumer;
	
	private String topicName;
	
	private int queue;
	
	private String consumerGroupName;

	public MQConsumer getConsumer() {
		return consumer;
	}

	public void setConsumer(MQConsumer consumer) {
		this.consumer = consumer;
	}

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

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}

}
