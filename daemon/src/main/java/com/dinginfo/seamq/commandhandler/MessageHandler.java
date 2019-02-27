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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.ServiceContext;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.service.NotifyService;
import com.dinginfo.seamq.service.QueueService;

public class MessageHandler implements CommandHandler,ResponseStatus {
	private final static Logger logger = LogManager.getLogger(MessageHandler.class);

	public static final int ACTION_PUT = 1;
	public static final int ACTION_GET = 2;
	public static final int ACTION_DELETE = 3;

	private int action = 0;

	private QueueService queueService =null;
	
	private NotifyService notifyService = null;

	public MessageHandler(int actionType) {
		this.queueService = MyBean.getBean(QueueService.BEAN_NAME,
				QueueService.class);
		this.action = actionType;
	}

	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {
		String topicId = request.getStringField(MessageMapping.FIELD_TOPIC_ID);
		String queueId = request.getStringField(MessageMapping.FIELD_QUEUE_ID);
		int queueIndex = request.getIntField(MessageMapping.FIELD_QUEUE);
		
		User user = request.getUser();
		
		boolean permission = false;
		try {
			MQueue queue = new MQueue(queueId);
			MQTopic topic = new MQTopic();
			topic.setId(topicId);
			queue.setTopic(topic);
			queue.setQueue(queueIndex);
			permission = queueService.checkPermission(user, queue);
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
			return;
		}

		if (!permission) {
			String error = "not permission for this topic";
			response.writeMessage(STATUS_ERROR, error);
			return;
		}

		switch(action){
		case ACTION_GET:
			get(request, response);
			break;
		case ACTION_PUT:
			put(request, response);
			break;
		case ACTION_DELETE:
			delete(request, response);
			break;
		}
	}

	

	private void put(MessageRequest request, MessageResponse response) {
		MQMessageExt msg = new MQMessageExt();
		
		msg.setData(request.getData());
		msg.setVersion(request.getStringField(MessageMapping.FIELD_VERSION));
		msg.setArraySize(request.getIntField(MessageMapping.FIELD_ARRAY_SIZE));
		MQueue queue = new MQueue(request.getStringField(MessageMapping.FIELD_QUEUE_ID));
		queue.setQueue(request.getIntField(MessageMapping.FIELD_QUEUE));
		msg.setQueue(queue);	
		ServiceContext context = request.getServerContext();
		msg.setVersion(context.getVersion());
		User user = request.getUser();
		Date createdTime =Calendar.getInstance().getTime();
		msg.setCreatedTime(createdTime);
		Map<String,DataField> attMap = request.getAttributeMap();
		if(attMap!=null){
			for(DataField bean : attMap.values()){
				msg.putAttribute(bean.getKey(), bean.getString());
				
			}
		}
		MQMessageExt message =null;
		try {			
			MQueue q = new MQueue(msg.getQueue().getId());
			MQueueAgent queueAgent = queueService.getQueueAgent(q);
			if(queueAgent==null){
				response.writeMessage(STATUS_ERROR, "not queue");
				return ;
			}
			String topicId = queueAgent.getTopic().getId();
			boolean inPermission = queueService.checkInPermission(user, topicId);
			if(!inPermission){
				response.writeMessage(STATUS_ERROR, "not in permission for this topic");
				return ;
			}
			if(MQTopic.STATUS_DISABLE.equals(queueAgent.getStatus())){
				response.writeMessage(STATUS_DISABLE, "queue status is disable");
				return;
			}
			msg.setDomain(new MQDomain(queueAgent.getDomain().getId()));
			msg.setTopic(new MQTopic(queueAgent.getTopic().getId()));
			message = queueAgent.put(msg);
			if (message != null) {
				response.setField(MessageMapping.FIELD_MESSAGE_ID, message.getMessageId());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
			sendNotify(request, queueAgent);
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void sendNotify(MessageRequest request,MQueueAgent queueAgent) throws Exception {
		ServiceContext context = request.getServerContext();
		if(!context.isNotifyMessage()) {
			return;
		}
		if(notifyService==null) {
			ServiceContext sc = request.getServerContext();
			notifyService = sc.getBean(NotifyService.BEAN_NAME, NotifyService.class);
		}
		long notifyOffset = 0;
		notifyOffset = 0;
		String topicId = queueAgent.getTopic().getId();
		List<String> consumerGroupNameList = queueService.getConsumerGroupNameList(topicId);
		if(consumerGroupNameList==null) {
			return ;
		}
		
		for(String groupName : consumerGroupNameList) {
			notifyOffset= queueAgent.getNotifyOffset(groupName);
			if(notifyOffset==1 || notifyOffset==2) {	
				MQueue notifyQueue = new MQueue();
				notifyQueue.setQueue(queueAgent.getQueue().getQueue());
				notifyQueue.setId(queueAgent.getQueue().getId());
				MQTopic notifyTopic = new MQTopic();
				notifyTopic.setName(queueAgent.getTopic().getName());
				notifyTopic.setId(queueAgent.getTopic().getId());
				notifyQueue.setTopic(notifyTopic);
				MQDomain notifyDomain = new MQDomain();
				notifyDomain.setId(queueAgent.getDomain().getId());
				notifyQueue.setDomain(notifyDomain);
				notifyService.sendMessage(notifyQueue);
				break;
			}
		}
	}

	private void get(MessageRequest request, MessageResponse response) {
		MQMessageExt msg = new MQMessageExt();
		MQueue queue = new MQueue();
		queue.setId(request.getStringField(MessageMapping.FIELD_QUEUE_ID));
		queue.setQueue(request.getIntField(MessageMapping.FIELD_QUEUE));
		msg.setQueue(queue);
		MQTopic topic = new MQTopic();
		topic.setId(request.getStringAttribute(MessageMapping.FIELD_TOPIC_ID));
		msg.setTopic(topic);
		msg.setDomain(new MQDomain(request.getLongField(MessageMapping.FIELD_DOMAIN_ID)));
		msg.setSessionId(request.getSessionId());
		User user =request.getUser();
		String consumerGroupName = request.getStringField(MessageMapping.FIELD_CONSUMER_GROUP);
		if(consumerGroupName==null || consumerGroupName.trim().length()==0) {
			consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		}
		msg.setConsumerGroupName(consumerGroupName);
		MQMessageExt message =null;
		try {
			MQueue q = new MQueue(msg.getQueue().getId());
			q.setDomain(msg.getDomain());
			q.setTopic(msg.getTopic());
			q.setQueue(msg.getQueue().getQueue());
			MQueueAgent queueAgent = queueService.getQueueAgent(q);
			if(queueAgent==null){
				response.writeMessage(STATUS_ERROR, "not queue");
				return ;
			}
			ConsumerGroup cgroup = new ConsumerGroup();
			cgroup.setTopic(new MQTopic(queueAgent.getTopic().getId()));
			cgroup.setName(consumerGroupName);
			boolean outPermission = queueService.checkOutPermission(user, cgroup);
			if(!outPermission){
				response.writeMessage(STATUS_ERROR, "not out permission for this topic");
				return ;
			}
			ServiceContext context = request.getServerContext();
			ServerConfig config = context.getConfig();
			queueAgent.setSessionTime(config.getSessionTime());
			message = queueAgent.get(msg);
			if (message != null) {
				response.setData(message.getData());
				response.setField(MessageMapping.FIELD_QUEUE_ID, message.getQueue().getId());
				response.setField(MessageMapping.FIELD_OFFSET, message.getOffset());
				response.setField(MessageMapping.FIELD_MESSAGE_ID, message.getMessageId());
				response.setField(MessageMapping.FIELD_TOPIC_NAME, message.getTopic().getName());
				response.setField(MessageMapping.FIELD_ARRAY_SIZE, message.getArraySize());
				response.setField(MessageMapping.FIELD_QUEUE, message.getQueue().getQueue());
				List<DataField> attributeList = message.getAttributeList();
				if (attributeList != null && attributeList.size() > 0) {
					for (DataField attribute : attributeList) {
						response.setAttribute(attribute.getKey(), attribute.getString());
					}
				}
				response.setStatus(STATUS_SUCCESS);
			} else {
				response.setStatus(STATUS_NOTMESSAGE);
			}
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}

	private void delete(MessageRequest request, MessageResponse response) {

		MQMessageExt msg = new MQMessageExt();
		msg.setMessageId(request.getStringField(MessageMapping.FIELD_MESSAGE_ID));
		msg.setDomain(new MQDomain(request.getLongField(MessageMapping.FIELD_DOMAIN_ID)));
		MQTopic topic = new MQTopic();
		topic.setId(request.getStringAttribute(MessageMapping.FIELD_TOPIC_ID));
		msg.setTopic(topic);
		MQueue queue = new MQueue();
		queue.setId(request.getStringField(MessageMapping.FIELD_QUEUE_ID));
		queue.setQueue(request.getIntField(MessageMapping.FIELD_QUEUE));
		msg.setQueue(queue);
		msg.setSessionId(request.getSessionId());
		User user = request.getUser();
		String consumerGroupName = request.getStringField(MessageMapping.FIELD_CONSUMER_GROUP);
		if(consumerGroupName==null || consumerGroupName.trim().length()==0) {
			consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		}
		msg.setConsumerGroupName(consumerGroupName);
		MQMessageExt message =null;
		int n = 0;
		try {
			MQueue q = new MQueue(msg.getQueue().getId());
			q.setDomain(msg.getDomain());
			q.setTopic(msg.getTopic());
			q.setQueue(msg.getQueue().getQueue());
			MQueueAgent queueAgent = queueService.getQueueAgent(q);
			if(queueAgent==null){
				response.writeMessage(STATUS_ERROR, "not queue");
				return ;
			}
			ConsumerGroup cgroup = new ConsumerGroup();
			cgroup.setTopic(new MQTopic(queueAgent.getTopic().getId()));
			cgroup.setName(consumerGroupName);
			boolean outPermission = queueService.checkOutPermission(user, cgroup);
			if(!outPermission){
				response.writeMessage(STATUS_ERROR, "not out permission for this topic");
				return ;
			}
			n = queueAgent.delete(msg);
			response.setAttribute(MessageMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
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
