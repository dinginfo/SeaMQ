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
import com.dinginfo.seamq.common.StringUtil;

public class MQTopic implements Serializable{
	public static final String TYPE_PEER_TO_PEER="P2P";
	
	public static final String TYPE_PUBLISH_SUBSCRIBE="P2S";
	
	public static final String STATUS_DISABLE="DISABLE";
	
	public static final String STATUS_ENABLE="ENABLE";
	
	private MQDomain domain;
	
	private String id;

	private String name;

	private int queueNum;

	private String remark;
	
	private String grantType;

	private User user;
	
	private ConsumerGroup consumerGroup;
	
	private String status;

	private Date createdTime;

	private Date updatedTime;
	
	public MQTopic(){
	}
	
	public MQTopic(String topicId){
		this.id = topicId;
	}

	public MQTopic(long domainId,String name) {
		if (name == null) {
			return;
		}
		this.domain = new MQDomain(domainId);
		this.name = name;
		this.queueNum = 1;
	}

	public String buildTopicId()throws Exception {
		if(domain==null){
			throw new Exception("domain is null");
		}
		if(name==null || name.trim().length()==0){
			throw new Exception("topic name is null");
		}
		StringBuilder sb = new StringBuilder(100);
		sb.append(String.valueOf(domain.getId()));
		sb.append(MQConstant.SEPARATOR);
		sb.append(name);
		this.id = StringUtil.generateMD5String(sb.toString());
		
		return id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getQueueNum() {
		return queueNum;
	}

	public void setQueueNum(int queueNum) {
		this.queueNum = queueNum;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public Date getUpdatedTime() {
		return updatedTime;
	}

	public void setUpdatedTime(Date updatedTime) {
		this.updatedTime = updatedTime;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public ConsumerGroup getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(ConsumerGroup consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public MQDomain getDomain() {
		return domain;
	}

	public void setDomain(MQDomain domain) {
		this.domain = domain;
	}

	public String getGrantType() {
		return grantType;
	}

	public void setGrantType(String grantType) {
		this.grantType = grantType;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("id:");
		sb.append(id);
		sb.append(",name:");
		sb.append(name);
		sb.append(",domain:");
		if(domain!=null){
			sb.append(domain.getId());	
		}
		sb.append(",queueNum:");
		sb.append(queueNum);
		if(status!=null && status.trim().length()>0){
			sb.append(",status:");
			sb.append(status);
		}
		return sb.toString();
	}
}
