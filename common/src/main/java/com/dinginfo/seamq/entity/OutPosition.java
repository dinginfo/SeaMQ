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
package com.dinginfo.seamq.entity;

import java.io.Serializable;
import java.util.Date;

import com.dinginfo.seamq.MQConstant;

public class OutPosition implements Serializable {
	private String id;
	
	private String groupName;
	
	private String queueId;
	
	private String topicId;
	
	private String sessionId;
	
	private String messageId;
	
	private Date updateTime;
	
	private long outOffset;

	public OutPosition() {}
	
	public OutPosition(String objId) {
		this.id = objId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	
	public long getOutOffset() {
		return outOffset;
	}

	public void setOutOffset(long outOffset) {
		this.outOffset = outOffset;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getQueueId() {
		return queueId;
	}

	public void setQueueId(String queueId) {
		this.queueId = queueId;
	}

	public String getTopicId() {
		return topicId;
	}

	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}

	public String buildId(String queueId,String name) {
		StringBuilder sb = new StringBuilder(100);
		sb.append(queueId);
		sb.append(MQConstant.SEPARATOR);
		sb.append(name);
		return sb.toString();
	}
}
