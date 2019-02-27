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
import java.util.List;

import com.dinginfo.seamq.MQConstant;
import com.dinginfo.seamq.common.StringUtil;

public class MQueue implements Serializable{
	private MQDomain domain;
	
	private MQTopic topic;
	
	private String id;

	private int queue;
	
	private String type;
	
	private Date createdTime;

	private long inOffset;

	private long outOffset;
	
	private String location;
	
	private String status;
	
	private NodeInfo nodeInfo;
	
	private List<OutPosition> outPositionList;

	public MQueue() {
	}
	
	public MQueue(String id) {
		this.id = id;
	}
	
	public MQueue(String topicId,int queue) {
		this.topic = new MQTopic();
		this.topic.setId(topicId);
		this.queue = queue;
	}
	
	public MQueue(MQTopic topic,int queue) {
		this.topic = topic;
		this.queue = queue;
	}
	
	public String buildQueueId()throws Exception{
		if(topic==null || topic.getId().trim().length()==0){
			throw new Exception("topic is null");
		}
		StringBuilder sb = new StringBuilder(100);
		sb.append(topic.getId());
		sb.append(MQConstant.SEPARATOR);
		sb.append(String.valueOf(queue));
		id =StringUtil.generateMD5String(sb.toString());
		return id;
	}


	public MQTopic getTopic() {
		return topic;
	}

	public void setTopic(MQTopic topic) {
		this.topic = topic;
	}


	public int getQueue() {
		return queue;
	}

	public void setQueue(int queue) {
		this.queue = queue;
	}


	public long getInOffset() {
		return inOffset;
	}

	public void setInOffset(long inOffset) {
		this.inOffset = inOffset;
	}

	public long getOutOffset() {
		return outOffset;
	}

	public void setOutOffset(long outOffset) {
		this.outOffset = outOffset;
	}

	public String getId() {
		if(id!=null) {
			return id;
		}
		try {
			buildQueueId();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}
	

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}


	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}


	public MQDomain getDomain() {
		return domain;
	}

	public void setDomain(MQDomain domain) {
		this.domain = domain;
	}

	
	public NodeInfo getNodeInfo() {
		return nodeInfo;
	}

	public void setNodeInfo(NodeInfo nodeInfo) {
		this.nodeInfo = nodeInfo;
	}

	
	public List<OutPosition> getOutPositionList() {
		return outPositionList;
	}

	public void setOutPositionList(List<OutPosition> outPositionList) {
		this.outPositionList = outPositionList;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(200);
		sb.append("queueId:");
		sb.append(id);
		sb.append(",domainId:");
		if(domain!=null){
			sb.append(domain.getId());	
		}
		sb.append(",topicId:");
		if(topic!=null){
			sb.append(topic.getId());	
		}
		
		if(status!=null && status.trim().length()>0){
			sb.append(",status:");
			sb.append(status);
		}
		if(location!=null && location.trim().length()>0){
			sb.append(",location:");
			sb.append(location);
		}
		sb.append(",inoffset:");
		sb.append(String.valueOf(inOffset));
		sb.append(",outoffset:");
		sb.append(String.valueOf(outOffset));
		return sb.toString();
	}
	

}
