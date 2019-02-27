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

package com.dinginfo.seamq;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;

public class MQMessageExt extends MQMessage {
	private MQDomain domain;
	
	private MQueue queue;
	
	private String version;
	
	private String msgId;

	private long offset;
	
	private int arraySize;
	
	private int arrayIndex;
	
	private Date createdTime;
	
	private String sessionId;
	
	private String consumerGroupName;
	
	private List<MQMessage> msgList;

	
	public MQMessageExt(MQMessage message){
		this.data = message.getData();
		this.attributeMap = new HashMap<String, DataField>();
		List<DataField> attributes = message.getAttributeList();
		if(attributes!=null){
			for(DataField attribute : attributes){
				attributeMap.put(attribute.getKey(), attribute);
			}
		}
	}
	
	public MQMessageExt(){}
	
	
	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public MQDomain getDomain() {
		return domain;
	}

	public void setDomain(MQDomain domain) {
		this.domain = domain;
	}

	public MQTopic getTopic() {
		return topic;
	}

	public void setTopic(MQTopic topic) {
		this.topic = topic;
	}

	public MQueue getQueue() {
		return queue;
	}

	public void setQueue(MQueue queue) {
		this.queue = queue;
	}

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long id) {
		this.offset = id;
	}

	public String buildMessageId()throws Exception {
		if (queue == null || queue.getId().trim().length()==0) {
			throw new Exception("queueId is null");
		}
		StringBuilder sb = new StringBuilder(100);
		sb.append(queue.getId());
		sb.append(":");
		sb.append(String.valueOf(offset));
		msgId = StringUtil.generateMD5String(sb.toString());
		return msgId;
	}

	

	@Override
	public String getMessageId() {
		return msgId;
	}

	public void setMessageId(String id) {
		this.msgId = id;
	}


	public int getArraySize() {
		return arraySize;
	}

	public void setArraySize(int arraySize) {
		this.arraySize = arraySize;
	}

	public int getArrayIndex() {
		return arrayIndex;
	}

	public void setArrayIndex(int arrayIndex) {
		this.arrayIndex = arrayIndex;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public boolean isMessageArray() {
		if(arraySize>0){
			return true;
		}else{
			return false;	
		}
	}
	
	@Override
	public List<MQMessage> getMessageList() {
		return msgList;
	}

	public void setMessageList(List<MQMessage> messageList) {
		this.msgList = messageList;
		if(msgList!=null && msgList.size()>0){
			this.arraySize = messageList.size();
		}else{
			this.arraySize=0;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if(topic!=null){
			sb.append(topic.getId());	
		}
		
		sb.append(",");
		if(queue!=null){
			sb.append(queue.getQueue());	
		}
		
		sb.append(",");
		sb.append(String.valueOf(offset));
		return sb.toString();
	}

}
