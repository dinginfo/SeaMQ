package com.dinginfo.seamq.service.jdbc;

import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.service.QueueAgentBuilder;
import com.dinginfo.seamq.storage.hbase.HBaseDataSource;
import com.dinginfo.seamq.storage.hbase.HBaseQueueAgent;
import com.dinginfo.seamq.storage.jdbc.DBQueueAgent;
import com.dinginfo.seamq.storage.jdbc.MessageDBStorage;
import com.dinginfo.seamq.storage.jdbc.TopicDBStorage;

public class QueueAgentDBBuilder implements QueueAgentBuilder {
	
	private MessageDBStorage messageStorage;
	
	private TopicDBStorage topicStorage; 


	public void setMessageStorage(MessageDBStorage messageStorage) {
		this.messageStorage = messageStorage;
	}

	public void setTopicStorage(TopicDBStorage topicStorage) {
		this.topicStorage = topicStorage;
	}



	@Override
	public MQueueAgent build() {
		DBQueueAgent queueAgent = new DBQueueAgent(); 
		queueAgent.setMessageStorage(messageStorage);
		queueAgent.setTopicStorage(topicStorage);
		return queueAgent;
	}

}
