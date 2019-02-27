package com.dinginfo.seamq.service.hbase;

import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.service.QueueAgentBuilder;
import com.dinginfo.seamq.storage.hbase.HBaseDataSource;
import com.dinginfo.seamq.storage.hbase.HBaseQueueAgent;
import com.dinginfo.seamq.storage.hbase.MessageHBaseStorage;
import com.dinginfo.seamq.storage.hbase.TopicHBaseStorage;

public class QueueAgentHBaseBuilder implements QueueAgentBuilder {
	private HBaseDataSource dataSource;
	
	private MessageHBaseStorage messageStorage;
	
	private TopicHBaseStorage topicStorage; 
	
	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}


	public void setMessageStorage(MessageHBaseStorage messageStorage) {
		this.messageStorage = messageStorage;
	}


	public void setTopicStorage(TopicHBaseStorage topicStorage) {
		this.topicStorage = topicStorage;
	}


	@Override
	public MQueueAgent build() {
		HBaseQueueAgent queueAgent = new HBaseQueueAgent(); 
		queueAgent.setMessageStorage(messageStorage);
		queueAgent.setTopicStorage(topicStorage);
		return queueAgent;
	}

}
