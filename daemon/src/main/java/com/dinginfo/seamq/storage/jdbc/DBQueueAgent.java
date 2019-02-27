/*
 * Copyright 2016 David Ding.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.dinginfo.seamq.storage.jdbc;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.MessageStatus;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;

public class DBQueueAgent extends MQueueAgent implements Serializable {
	
	protected MessageDBStorage messageStorage;

	protected TopicDBStorage topicStorage;

	public DBQueueAgent() {
		this.inOffset = 0;
		this.outOffset = 0;
		this.inLock = new ReentrantReadWriteLock();
		this.outLock = new ReentrantReadWriteLock();
	}
	
	
	public void setMessageStorage(MessageDBStorage messageStorage) {
		this.messageStorage = messageStorage;
	}
	
	
	public void setTopicStorage(TopicDBStorage topicStorage) {
		this.topicStorage = topicStorage;
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
		return messageStorage.delete(message);
	}


	@Override
	public MessageStatus putToStorage(MQMessageExt message) throws Exception {
		return messageStorage.put(message);
	}

	@Override
	protected int increaseOutPosition(OutPosition out) throws Exception {
		return topicStorage.increaseOutPosition(out);
	}

	@Override
	protected OutPosition loadOutPositionByPK(String queueId, String userId) throws Exception {
		OutPosition bean = new OutPosition();
		String id = bean.buildId(queueId, userId);
		bean.setId(id);
		return topicStorage.getOutPositionByPK(bean);
	}

	@Override
	protected MQTopic getTopicByPK(String topicId) throws Exception {
		return topicStorage.getTopicByPK(topicId);
	}


	@Override
	public boolean close() throws Exception {
		return false;
		
	}
	

}
