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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ServiceContext;
import com.dinginfo.seamq.client.MQClientService;
import com.dinginfo.seamq.client.MessageClient;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.TopicCommand;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.service.QueueService;
import com.dinginfo.seamq.service.TopicService;
import com.dinginfo.seamq.service.UserService;

public class TopicHandler implements CommandHandler,ResponseStatus {
	private final static Logger logger = LogManager.getLogger(TopicHandler.class);
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
	

	private TopicService topicService = null;
	
	private QueueService queueService = null;
	
	private UserService userService = null;

	private int action = 0;

	public TopicHandler(int actionType) {
		this.topicService = MyBean.getBean(TopicService.BEAN_NAME,
				TopicService.class);
		this.queueService = MyBean.getBean(QueueService.BEAN_NAME,
				QueueService.class);
		this.userService = MyBean.getBean(UserService.BEAN_NAME, UserService.class);
		this.action = actionType;
	}

	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {
		switch(action){
		case ACTION_GET:
			get(request,response);
			break;
		case ACTION_CREATE:
			create(request,response);
			break;
		case ACTION_UPDATE:
			update(request, response);
			break;
		case ACTION_DROP:
			dropTopic(request,response);
			break;
		case ACTION_DISABLE:
			disableTopic(request,response);
			break;
		case ACTION_ENABLE:
			enableTopic(request,response);
			break;
		case ACTION_DISABLE_QUEUE:
			disableQueue(request,response);
			break;
		case ACTION_DISABLE_QUEUE_STATUS:
			disableQueueStatus(request,response);
			break;
		case ACTION_ENABLE_QUEUE:
			enableQueue(request,response);
			break;
		case ACTION_INCREASE_QUEUE:
			increaseQueue(request,response);
			break;
		case ACTION_REDUCE_QUEUE:
			reduceQueue(request,response);
			break;
		case ACTION_COUNT:
			getCount(request, response);
			break;
		case ACTION_LIST:
			getTopicList(request, response);
			break;
		case ACTION_USER_LIST:
			getUserList(request, response);
			break;
		case ACTION_GRANT_USER_TO_PRODUCER:
			grantUserToProducer(request,response);
			break;
		case ACTION_REMOVE_PRODUCER_USER:
			removeUserFromProducer(request, response);
			break;
		case ACTION_GET_PRODUCER_USER_LIST:
			getProducerUserList(request, response);
			break;
		case ACTION_CREATE_CONSUMER_GROUP:
			createConsumerGroup(request, response);
			break;
		case ACTION_DROP_CONSUMER_GROUP:
			dropConsumerGroup(request, response);
			break;
		case ACTION_GET_CONSUMER_GROUP_LIST:
			getConsumerGroupList(request, response);
			break;
		case ACTION_GRANT_USER_TO_CONSUMER_GROUP:
			grantUserToConsumerGroup(request, response);
			break;
		case ACTION_REMOVE_CONSUMER_USER:
			removeConsumerUser(request, response);
			break;
		case ACTION_GET_CONSUMER_USER_LIST:
			getConsumerUserList(request, response);
			break;
		}

	}

	protected MQTopic mappingTopic(MessageRequest request) {
		MQTopic topic = new MQTopic();
		topic.setName(request.getStringAttribute(TopicMapping.FIELD_NAME));
		topic.setId(request.getStringAttribute(TopicMapping.FIELD_ID));
		topic.setQueueNum(request.getIntAttribute(TopicMapping.FIELD_QUEUE_NUM));
		topic.setRemark(request.getStringAttribute(TopicMapping.FIELD_REMARK));
		MQDomain domain = new MQDomain(request.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID));
		topic.setDomain(domain);
		User user = new User();
		user.setDomain(domain);
		user.setName(request.getStringAttribute(TopicMapping.FIELD_USERID));
		topic.setUser(user);
		topic.setGrantType(request.getStringAttribute(TopicMapping.FIELD_GRANTTYPE));
		return topic;
	}

	private void create(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			String topicId = topic.getId();
			if(topicId==null || topicId.trim().length()==0){
				topic.buildTopicId();
			}
			MQTopic t = topicService.getTopicByPK(topic.getId());
			if(t!=null){
				StringBuilder sb = new StringBuilder();
				sb.append("topic[");
				sb.append(topic.getName());
				sb.append("] is exist");
				response.writeMessage(STATUS_ERROR, sb.toString());
			}else{
				topicService.createTopic(topic);
				response.setStatus(STATUS_SUCCESS);
				response.writeMessage();	
			}
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void update(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			String topicId = topic.getId();
			int n =topicService.updateTopic(topic);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}

	private void get(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if (topic != null) {
				response.setAttribute(TopicMapping.FIELD_ID, topic.getId());
				response.setAttribute(TopicMapping.FIELD_NAME, topic.getName());
				response.setAttribute(TopicMapping.FIELD_QUEUE_NUM, topic.getQueueNum());
				MQDomain domain = topic.getDomain();
				if(domain!=null){
					response.setAttribute(DomainMapping.FIELD_DOMAIN_ID, domain.getId());	
				}
				response.setAttribute(TopicMapping.FIELD_STSTUS, topic.getStatus());
				response.setAttribute(TopicMapping.FIELD_REMARK, topic.getRemark());
				if (topic.getUser() != null && topic.getUser().getName() != null) {
					response.setAttribute(TopicMapping.FIELD_USERID, topic.getUser().getName());
				}
				response.setStatus(STATUS_SUCCESS);
			} else {
				response.setStatus(STATUS_ERROR);
				response.setException("not topic info");
			}
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
			
		}
	}

	private void dropTopic(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			
			topic =topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			String status = topic.getStatus();
			if(!MQTopic.STATUS_DISABLE.equals(status)){
				response.writeMessage(STATUS_ERROR, "please disable this topic first");
				return;
			}
			int n =topicService.dropTopic(topic);
			int queueNum = topic.getQueueNum();
			for(int i=0;i<queueNum;i++){
				removeQueueStatus(request, topic, i);
			}
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	
	private void disableTopic(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			
			int n =topicService.disableTopic(topic);
			int queueNum = topic.getQueueNum();
			for(int i=0;i<queueNum;i++){
				removeQueueStatus(request, topic, i);
			}
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	
	private void removeQueueStatus(MessageRequest request,MQTopic topic,int queueIndex){
		MQueue queue = new MQueue(topic.getId(),queueIndex);
		String queueId = queue.getId();
		ServiceContext context = request.getServerContext();
		MQClientService clientService = context.getBean(MQClientService.BEAN_NAME, MQClientService.class);
		String queueLocation = clientService.getQueueLocation(queueId);
		if(queueLocation==null || queueLocation.trim().length()==0){
			return;
		}
		if(!clientService.containsBrokerId(queueLocation)){
			return;
		}
		
		MessageClient brokerClient = null;
		try {
			brokerClient = clientService.getMessageClient(queueLocation);
		} catch (Exception e) {
			log(e);
		}
		if(brokerClient==null){
			return ;
		}
		
		MQTopic t = new MQTopic();
		t.setId(topic.getId());
		topic.setQueueNum(queueIndex);
		ClientCommand command =  new TopicCommand(TopicCommand.ACTION_DISABLE_QUEUE_STATUS, t);
		command.setSessionId(request.getSessionId());
		brokerClient.writeMessage(command);
	}
	
	private void enableTopic(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			int n =topicService.enableTopic(topic);
			int queueNum = topic.getQueueNum();
			for(int i=0;i<queueNum;i++){
				removeQueueStatus(request, topic, i);
				
			}
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());	
		}
	}
	
	private void disableQueue(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			int queueNum = topic.getQueueNum();
			if(queueNum==1){
				response.writeMessage(STATUS_ERROR, "queue num is 1, can not disable queue");
				return;
			}
			
			MQueue queue = null;
			String queueId = null;
			queue = new MQueue(topic.getId(),queueNum-1);
			queueId = queue.buildQueueId();
			int n =topicService.disableQueue(queue);
			removeQueueStatus(request, topic, queueNum-1);
			response.setAttribute(TopicMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void disableQueueStatus(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}
		
		try {
			topic = topicService.getTopicByPK(topic.getId());
			if (topic == null) {
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			if(topic!=null){
				MQueue queue = new MQueue(topic.getId(), topic.getQueueNum()-1);
				String queueId = queue.getId();
				queueService.removeQueueStatus(queueId);	
			}
			response.setAttribute(TopicMapping.RESULT, 1);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());	
		}
	}
	
	private void enableQueue(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			
			int queueNum = topic.getQueueNum();
			if(queueNum==1){
				response.writeMessage(STATUS_ERROR, "queue num is 1, can not enable queue");
				return;
			}
			MQueue queue = new MQueue(topic.getId(),queueNum-1);
			String queueId = queue.buildQueueId();
			int n =topicService.enableQueue(queue);
			removeQueueStatus(request, topic,queueNum-1);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void increaseQueue(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			int queueNum = topic.getQueueNum();
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			if(MQTopic.STATUS_DISABLE.equals(topic.getStatus())){
				response.writeMessage(STATUS_ERROR, "topic is disable,unable to increase queue");
				return;
			}
			int qnum = topic.getQueueNum();
			if(qnum>1){
				MQueue queue = new MQueue(topic.getId(), qnum-1);
				String queueId = queue.buildQueueId();
				MQueue q = topicService.getQueueByPK(queue);
				if(q!=null && MQTopic.STATUS_DISABLE.equals(q.getStatus())){
					response.writeMessage(STATUS_ERROR, "queue is disable,unable to increase queue");
					return;
				}
			}
			
			int n=topicService.increaseQueue(topic, queueNum);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void reduceQueue(MessageRequest request, MessageResponse response) {
		
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			topic = topicService.getTopicByPK(topic.getId());
			if(topic==null){
				response.writeMessage(STATUS_ERROR, "not topic info");
				return;
			}
			
			int queueNum = topic.getQueueNum();
			if(queueNum==1){
				response.writeMessage(STATUS_ERROR, "queue num is 1, can not disable queue");
				return;
			}
			MQueue queue = new MQueue(topic.getId(),queueNum-1);
			String queueId = queue.getId();
			
			queue = topicService.getQueueByPK(queue);
			if(queue==null){
				response.writeMessage(STATUS_ERROR, "not queue info:"+(queueNum-1));
				return;
			}
			String queueStatus = queue.getStatus();
			if(!MQTopic.STATUS_DISABLE.equals(queueStatus)){
				response.writeMessage(STATUS_ERROR, "please disable this queue first");
				return;
			}
			int n =topicService.reduceQueue(topic, queueId);
			removeQueueStatus(request, topic, queueNum-1);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();

		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void getCount(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			
			long n = topicService.getTopicCount();
			response.setAttribute(TopicMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void getTopicList(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			int pageSize =request.getIntAttribute(TopicMapping.FIELD_PAGE_SIZE);;
			int pageNo =request.getIntAttribute(TopicMapping.FIELD_PAGE_NO);
			List<MQTopic> topicList = null;
			topicList = topicService.getTopicList(topic,pageSize,pageNo);
			if(topicList!=null && topicList.size()>0){
				String jsonString = JSON.toJSONString(topicList);
				response.setData(jsonString.getBytes());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void getUserList(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			List<User> userList = topicService.getUserListByTopic(topic.getId());
			if(userList!=null && userList.size()>0){
				String jsonString = JSON.toJSONString(userList);
				response.setData(jsonString.getBytes());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void grantUserToProducer(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			User user = userService.getUserByPK(topic.getUser());
			if(user==null) {
				StringBuilder sb = new StringBuilder(100);
				sb.append("not user:");
				sb.append(topic.getUser().getName());
				response.writeMessage(STATUS_ERROR, sb.toString());
				return;
			}
			if(topicService.getProducerUserByPK(topic)!=null) {
				StringBuilder sb = new StringBuilder(100);
				sb.append("user ");
				sb.append(topic.getUser().getName());
				sb.append(" is exist in this producer");
				response.writeMessage(STATUS_ERROR, sb.toString());
				return;
			}
			MQTopic t= topicService.getTopicByPK(topic.getId());
			t.setUser(topic.getUser());
			int n=topicService.grantUserToProducer(t);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void removeUserFromProducer(MessageRequest request, MessageResponse response) {
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			MQTopic t= topicService.getTopicByPK(topic.getId());
			t.setUser(topic.getUser());
			int n=topicService.removeProducerUser(t);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void createConsumerGroup(MessageRequest request, MessageResponse response) {
		String groupName = request.getStringAttribute(TopicMapping.FIELD_CONSUMER_GROUP_NAME);
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			MQTopic t= topicService.getTopicByPK(topic.getId());
			t.setUser(topic.getUser());
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(t);
			group.setName(groupName);
			if(topicService.getConsumerGroupByPK(group)!=null) {
				StringBuilder sb = new StringBuilder(100);
				sb.append("consumer group name ");
				sb.append(groupName);
				sb.append(" is exist");
				response.writeMessage(STATUS_ERROR, sb.toString());
				return;
			}
			topicService.createConsumerGroup(group);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void dropConsumerGroup(MessageRequest request, MessageResponse response) {
		String groupName = request.getStringAttribute(TopicMapping.FIELD_CONSUMER_GROUP_NAME);
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			MQTopic t= topicService.getTopicByPK(topic.getId());
			t.setUser(topic.getUser());
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(t);
			group.setName(groupName);
			
			int n =topicService.dropConsumerGroupByPK(group);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void getConsumerGroupList(MessageRequest request, MessageResponse response){
		String groupName = request.getStringAttribute(TopicMapping.FIELD_CONSUMER_GROUP_NAME);
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			List<ConsumerGroup> groupList =topicService.getConsumerGroupList(topic.getId());
			if(groupList!=null && groupList.size()>0) {
				String jsonString = JSON.toJSONString(groupList);
				response.setData(jsonString.getBytes());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
		
	}
	

	
	public void getProducerUserList(MessageRequest request, MessageResponse response){
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			List<User> userList = topicService.getProducerUserList(topic.getId());
			if(userList!=null && userList.size()>0) {
				String jsonString = JSON.toJSONString(userList);
				response.setData(jsonString.getBytes());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}	
	}
	
	public void grantUserToConsumerGroup(MessageRequest request, MessageResponse response){
		String groupName = request.getStringAttribute(TopicMapping.FIELD_CONSUMER_GROUP_NAME);
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			User user = userService.getUserByPK(topic.getUser());
			if(user==null) {
				StringBuilder sb = new StringBuilder(100);
				sb.append("not user:");
				sb.append(topic.getUser().getName());
				response.writeMessage(STATUS_ERROR, sb.toString());
				return;
			}
			MQTopic t = topicService.getTopicByPK(topic.getId());
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(t);
			group.setName(groupName);
			group.putUser(topic.getUser());
			if(topicService.getConsumerUserByPK(group)!=null) {
				StringBuilder sb = new StringBuilder(100);
				sb.append("user ");
				sb.append(topic.getUser().getName());
				sb.append(" is exist in this consumer group");
				response.writeMessage(STATUS_ERROR, sb.toString());
				return;
			}
			int n = topicService.grantUserToConsumerGroup(group);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
		
	}
	
	public void removeConsumerUser(MessageRequest request, MessageResponse response){
		String groupName = request.getStringAttribute(TopicMapping.FIELD_CONSUMER_GROUP_NAME);
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			MQTopic t = topicService.getTopicByPK(topic.getId());
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(t);
			group.setName(groupName);
			group.putUser(topic.getUser());
			int n = topicService.removeConsumerUser(group);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(TopicMapping.RESULT, n);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
		
	}
	
	public void getConsumerUserList(MessageRequest request, MessageResponse response){
		MQTopic topic = mappingTopic(request);
		if (topic == null) {
			response.writeMessage(STATUS_ERROR, "not topic info");
			return;
		}

		try {
			List<User> userList = topicService.getConsumerUserList(topic.getId());
			if(userList!=null && userList.size()>0) {
				String jsonString = JSON.toJSONString(userList);
				response.setData(jsonString.getBytes());
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			log(e);
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}	
	}
	
	private void log(Exception e){
		StringWriter writer = new StringWriter();
		PrintWriter pw = new PrintWriter(writer);
		e.printStackTrace(pw);
		logger.error(writer.toString());
	}

}
