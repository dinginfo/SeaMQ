package com.dinginfo.seamq.storage.hbase;

import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.MessageStatus;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;

public class HBaseQueueAgent extends MQueueAgent {
	private MessageHBaseStorage messageStorage;

	private TopicHBaseStorage topicStorage;
	
	public void setMessageStorage(MessageHBaseStorage messageStorage) {
		this.messageStorage = messageStorage;
	}

	public void setTopicStorage(TopicHBaseStorage topicStorage) {
		this.topicStorage = topicStorage;
	}


	@Override
	public MessageStatus putToStorage(MQMessageExt message) throws Exception {
		return messageStorage.put(message);
	}

	@Override
	public MQMessageExt getFromStorage(MQMessageExt message) throws Exception {
		return messageStorage.get(message);
	}

	@Override
	public MQueue loadQueueByPK(String queueId) throws Exception {
		return topicStorage.loadQueueByPK(queueId);
	}

	@Override
	public int deleteFromStroage(MQMessageExt message) throws Exception {
		messageStorage.delete(message);
		String queueId = message.getQueue().getId();
		OutPosition out = new OutPosition();
		out.setId(queueId);
		out.setOutOffset(message.getOffset());
		return topicStorage.increaseOutPosition(out);
	}

	
	@Override
	protected OutPosition loadOutPositionByPK(String queueId, String userId) throws Exception {
		OutPosition bean = new OutPosition();
		String key = bean.buildId(queueId, userId);
		return topicStorage.getOutPositionByPK(key);
	}

	@Override
	protected int increaseOutPosition(OutPosition out) throws Exception {
		return topicStorage.increaseOutPosition(out);
	}

	@Override
	protected MQTopic getTopicByPK(String topicId) throws Exception {
		return topicStorage.getTopicByPK(topicId);
	}

	@Override
	public boolean close() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	

}
