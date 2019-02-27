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

package com.dinginfo.seamq.commandhandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;
import com.dinginfo.seamq.entity.mapping.QueueMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.service.TopicService;

public class QueueHandler implements CommandHandler,ResponseStatus {
	private final static Logger logger = LogManager.getLogger(QueueHandler.class);
	public static final int ACTION_GET = 1;
	public static final int ACTION_SET = 2;
	public static final int ACTION_GET_LIST = 3;

	protected TopicService topicService = null;

	private int action;

	public QueueHandler(int actionType) {
		this.topicService =  MyBean.getBean(
				TopicService.BEAN_NAME, TopicService.class);
		this.action = actionType;
	}

	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {

		switch(action){
		case ACTION_GET:
			get(request, response);
			break;
		case ACTION_GET_LIST:
			getQueueList(request, response);
			break;
		}
	}

	protected MQueue mappingQueue(MessageRequest request) {
		String queueId = request.getStringAttribute(QueueMapping.FIELD_QUEUE_ID);
		if (queueId == null || queueId.trim().length() == 0) {
			return null;
		}

		MQueue queue = new MQueue(queueId);
		String topicId = request.getStringAttribute(QueueMapping.FIELD_TOPIC_ID);
		MQTopic topic = new MQTopic();
		topic.setId(topicId);
		queue.setTopic(topic);
		return queue;
	}
	
	private void get(MessageRequest request, MessageResponse response) { 
		MQueue q = mappingQueue(request);
		if (q == null) {
			response.writeMessage(STATUS_ERROR, "not queue info");
			return;
		}

		try {

			MQueue queue = topicService.getQueueByPK(q);
			if (queue != null) {
				response.setAttribute(QueueMapping.FIELD_QUEUE_ID, queue.getId());
				response.setAttribute(QueueMapping.FIELD_TOPIC_ID,queue.getTopic().getId());
				response.setAttribute(QueueMapping.FIELD_DOMAIN_ID, queue.getDomain().getId());
				response.setAttribute(QueueMapping.FIELD_IN_OFFSET, queue.getInOffset());
				response.setAttribute(QueueMapping.FIELD_OUT_OFFSET, queue.getOutOffset());
				response.setAttribute(QueueMapping.FIELD_STATUS, queue.getStatus());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void getQueueList(MessageRequest request, MessageResponse response) {
		MQueue queue = mappingQueue(request);
		if (queue == null) {
			response.writeMessage(STATUS_ERROR, "not queue info");
			return;
		}
		String topicId = queue.getTopic().getId();
		try{
			MQTopic topic = topicService.getTopicByPK(topicId);
			List<MQueue> queueList = topicService.getQueueList(topic);
			List<OutPosition> positionList = null;
			for(MQueue q : queueList) {
				positionList = topicService.getOutPositionList(q.getId());
				if(positionList!=null && positionList.size()>0) {
					q.setOutPositionList(positionList);
				}	
			}
			String jsonString = JSON.toJSONString(queueList);
			response.setData(jsonString.getBytes());
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		}catch(Exception e){
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
		
	}
	
	private void log(Exception e) {
		StringWriter writer = new StringWriter();
		PrintWriter pw = new PrintWriter(writer);
		e.printStackTrace(pw);
		logger.error(writer.toString());
	}

}
