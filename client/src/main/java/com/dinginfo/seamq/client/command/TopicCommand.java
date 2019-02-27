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

package com.dinginfo.seamq.client.command;

import com.dinginfo.seamq.common.ProtoUtil;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class TopicCommand extends AbstractCommand {
	public static final int ACTION_GET = 1;
	public static final int ACTION_CREATE = 2;
	public static final int ACTION_UPDATE = 3;
	public static final int ACTION_DROP = 4;
	public static final int ACTION_DISABLE=5;
	public static final int ACTION_ENABLE=6;
	public static final int ACTION_DISABLE_QUEUE=7;
	public static final int ACTION_DISABLE_QUEUE_STATUS=8;
	public static final int ACTION_ENABLE_QUEUE=9;
	public static final int ACTION_INCREASE_QUEUE=10;
	public static final int ACTION_REDUCE_QUEUE=11;
	public static final int ACTION_COUNT=12;
	public static final int ACTION_LIST=13;
	public static final int ACTION_USER_LIST=14;
	public static final int ACTION_GRANT_USER_TO_PRODUCER = 15;
	public static final int ACTION_REMOVE_PRODUCER_USER = 16;
	public static final int ACTION_GET_PRODUCER_USER_LIST = 17;
	public static final int ACTION_CREATE_CONSUMER_GROUP = 18;
	public static final int ACTION_DROP_CONSUMER_GROUP = 19;
	public static final int ACTION_GET_CONSUMER_GROUP_LIST = 20;
	public static final int ACTION_GRANT_USER_TO_CONSUMER_GROUP = 21;
	public static final int ACTION_REMOVE_CONSUMER_USER = 22;
	public static final int ACTION_GET_CONSUMER_USER_LIST = 23;
	

	protected MQTopic topic ;
	
	protected String serviceName;
	
	private int pageSize;
	
	private int pageNo;
	
	public TopicCommand(int actionType,MQTopic topic){
		this.topic = topic;
		switch(actionType){
		case ACTION_GET:
			serviceName = COMMAND_TOPIC_GET;
			break;
		case ACTION_CREATE:
			serviceName = COMMAND_TOPIC_CREATE;
			break;
		case ACTION_UPDATE:
			serviceName = COMMAND_TOPIC_UPDATE;
			break;
		case ACTION_DROP:
			serviceName = COMMAND_TOPIC_DROP;
			break;
		case ACTION_DISABLE :
			 serviceName = COMMAND_TOPIC_DISABLE;
			 break;
		case ACTION_ENABLE :
			serviceName = COMMAND_TOPIC_ENABLE;
			break;
		case ACTION_DISABLE_QUEUE:
			serviceName = COMMAND_TOPIC_DISABLE_QUEUE;
			break;
		case ACTION_ENABLE_QUEUE:
			serviceName = COMMAND_TOPIC_ENABLE_QUEUE;
			break;
		case ACTION_INCREASE_QUEUE:
			serviceName = COMMAND_TOPIC_INCREASE_QUEUE;
			break;
		case ACTION_REDUCE_QUEUE:
			serviceName = COMMAND_TOPIC_REDUCE_QUEUE;
			break;
		case ACTION_COUNT:
			serviceName = COMMAND_TOPIC_COUNT;
			break;
		case ACTION_LIST:
			serviceName = COMMAND_TOPIC_LIST;
			break;
		case ACTION_USER_LIST:
			serviceName = COMMAND_TOPIC_USER_LIST;
			break;
		case ACTION_DISABLE_QUEUE_STATUS:
			serviceName = COMMAND_TOPIC_DISABLE_QUEUE_STATUS;
			break;
		case ACTION_GRANT_USER_TO_PRODUCER:
			serviceName = COMMAND_TOPIC_GRANT_USER_TO_PRODUCER;
			break;
		case ACTION_REMOVE_PRODUCER_USER:
			serviceName = COMMAND_TOPIC_REMOVE_PRODUCER_USER;
			break;
		case ACTION_GET_PRODUCER_USER_LIST:
			serviceName = COMMAND_TOPIC_GET_PRODUCER_USER_LIST;
			break;
		case ACTION_CREATE_CONSUMER_GROUP:
			serviceName = COMMAND_TOPIC_CREATE_CONSUMER_GROUP;
			break;
		case ACTION_DROP_CONSUMER_GROUP:
			serviceName = COMMAND_TOPIC_DROP_CONSUMER_GROUP;
			break;
		case ACTION_GET_CONSUMER_GROUP_LIST:
			serviceName = COMMAND_TOPIC_GET_CONSUMER_GROUP_LIST;
			break;
		case ACTION_GRANT_USER_TO_CONSUMER_GROUP:
			serviceName = COMMAND_TOPIC_GRANT_USER_TO_CONSUMER_GROUP;
			break;
		case ACTION_REMOVE_CONSUMER_USER:
			serviceName = COMMAND_TOPIC_REMOVE_CONSUMER_USER;
			break;
		case ACTION_GET_CONSUMER_USER_LIST:
			serviceName = COMMAND_TOPIC_GET_CONSUMER_USER_LIST;
			break;
		}
		
	}
	
	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(serviceName);
		requestBuilder.setRequestId(this.uuid);
		if(topic==null){
			return requestBuilder.build();
		}
		if(sessionId!=null){
			requestBuilder.setSessionId(sessionId);
		}
		ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_ID, topic.getId());
		ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_NAME, topic.getName());
		ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_QUEUE_NUM, topic.getQueueNum());
		ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_REMARK, topic.getRemark());
		ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_GRANTTYPE, topic.getGrantType());
		if(topic.getConsumerGroup()!=null) {
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_CONSUMER_GROUP_NAME, topic.getConsumerGroup().getName());
		}
		if(topic.getUser()!=null){
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_USERID, topic.getUser().getName());
		}
		if(topic.getDomain()!=null){
			ProtoUtil.setAttribute(requestBuilder, DomainMapping.FIELD_DOMAIN_ID, topic.getDomain().getId());	
		}
		if(COMMAND_TOPIC_LIST.equals(serviceName)){
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_PAGE_SIZE, pageSize);
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_PAGE_NO, pageNo);
		}
		return requestBuilder.build(); 
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	
	

}
