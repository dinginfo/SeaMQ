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

package com.dinginfo.seamq.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.MQMessage;
import com.dinginfo.seamq.MQMessageArray;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.DelCommand;
import com.dinginfo.seamq.client.command.GetCommand;
import com.dinginfo.seamq.client.exception.ConnectionException;
import com.dinginfo.seamq.client.exception.TimeoutException;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.mapping.MessageMapping;

public class MQConsumer extends MQClient{
	
	protected MQConsumer(MQClientService service){
		this.clientService = service;
	}
	
	public MQMessage getMessage(String topicName)throws Exception{
		String consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		return getMessage(topicName, consumerGroupName);
	}
	
	public MQMessage getMessage(String topicName,String consumerGroupName)throws Exception{
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(consumerGroupName==null || consumerGroupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		MQTopic t = new MQTopic(domainId,topicName);
		String topicId = t.buildTopicId();

		MQTopic topic = clientService.getTopicInfo(domainId,topicId);
		if (topic == null) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("not topic :");
			sb.append(topicName);
			throw new Exception(sb.toString());
		}
		MQMessageExt msg = new MQMessageExt();
		MQTopic bean = new MQTopic();
		bean.setId(topicId);
		msg.setTopic(bean);
		int queueNum = topic.getQueueNum();
		if(queueNum==1){
			MQueue queue = new MQueue();
			queue.setQueue(0);
			msg.setQueue(queue);
			MQueue q = new MQueue(topicId, 0);
			String queueId = q.getId();
			return get(queueId,consumerGroupName);
		}
		List<String> qlist= new ArrayList<String>();
		
		for(int i=0;i<queueNum;i++){
			qlist.add(String.valueOf(i));
		}
		int n = qlist.size();
		int queueIndex =0;
		Random random = new Random();
		String queueId=null;
		String queueName = null;
		MQueue q =null;
		while(n>0){
			if(n==1){
				queueIndex=0;
			}else{
				queueIndex = random.nextInt(n);	
			}
			
			if(queueIndex<0){
				queueIndex = Math.abs(queueIndex);
			}
			queueName = qlist.get(queueIndex);
			q = new MQueue(topicId, Integer.parseInt(queueName));
			queueId = q.getId();
			msg = get(queueId,consumerGroupName);
			if(msg!=null){
				break;
			}else{
				qlist.remove(queueIndex);
				n = qlist.size();
			}
		}
		updateSession();
		return msg;
	}
	
	public MQMessage getMessage(String topicName,int queueIndex)throws Exception{
		String consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		return getMessage(topicName, queueIndex, consumerGroupName);
	}
	
	public MQMessage getMessage(String topicName,int queueIndex,String consumerGroupName)throws Exception{
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(consumerGroupName==null || consumerGroupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		if(queueIndex<0){
			throw new Exception("queue is invalid");
		}
		MQTopic t = new MQTopic(domainId,topicName);
		String topicId = t.buildTopicId();
		MQTopic topic = clientService.getTopicInfo(domainId,topicId);
		if (topic == null) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("not topic :");
			sb.append(topicName);
			throw new Exception(sb.toString());
		}
		
		//System.out.println("topic:"+topic);
		int queueNum = topic.getQueueNum();
		if(queueIndex>=queueNum){
			throw new Exception("queue is invalid");
		}
		MQMessageExt msg = new MQMessageExt();
		t=new MQTopic();
		t.setId(topicId);
		msg.setTopic(t);
		MQueue q = new MQueue();
		q.setQueue(queueIndex);
		msg.setQueue(q);
		q = new MQueue(topicId, queueIndex);
		String queueId = q.getId();
		MQMessageExt messageExt =get(queueId,consumerGroupName);
		updateSession();
		return messageExt;
	}
	
	private MQMessageExt get(String queueId,String consumerGroupName)throws Exception{		
		MessageClient client = getMessageClientByQueueId(queueId);
		if(client==null){
			return null;
		}
		if(!client.isContected()){
			return null;
		}
		MQMessageExt msg = new MQMessageExt();
		msg.setQueue(new MQueue(queueId));
		msg.setConsumerGroupName(consumerGroupName.toLowerCase());
		ClientCommand command = new GetCommand(msg);
		command.setSessionId(session.getId());
		MQResponse response = client.writeMessage(command);
		if(response==null){
			throw new ConnectionException("not connect to broker server");
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			msg = new MQMessageExt();
			MQTopic topic = new MQTopic();
			topic.setId(response.getStringField(MessageMapping.FIELD_TOPIC_ID));
			topic.setName(response.getStringField(MessageMapping.FIELD_TOPIC_NAME));
			msg.setTopic(topic);
			MQueue queue = new  MQueue();
			queue.setId(response.getStringField(MessageMapping.FIELD_QUEUE_ID));
			queue.setQueue(response.getIntField(MessageMapping.FIELD_QUEUE));
			msg.setQueue(queue);
			msg.setOffset(response.getIntField(MessageMapping.FIELD_OFFSET));
			msg.setMessageId(response.getStringField(MessageMapping.FIELD_MESSAGE_ID));
			msg.setArraySize(response.getIntField(MessageMapping.FIELD_ARRAY_SIZE));
			byte[] data = response.getData();
			if(msg.getArraySize()>0){
				MQMessageArray msgArray = new MQMessageArray();
				msgArray.buildMessage(data);
				msg.setMessageList(msgArray.getMessageList());
			}else{
				msg.setData(data);
			}
			Map<String,DataField> attMap = response.getAttributeMap();
			if(attMap!=null && attMap.size()>0){
				for(DataField attBean : attMap.values()){
					msg.putAttribute(attBean.getKey(), attBean.getString());
				}
			}
			return msg;	
		}
		
		if(ResponseStatus.STATUS_NOTMESSAGE.equals(response.getStatus())){
			return null;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return null;
	}
	
	
	public int delete(MQMessage message)throws Exception{
		if(message==null || message.getMessageId()==null){
			throw new Exception("Message id is null");
		}
		MQMessageExt msg =(MQMessageExt)message;
		String queueId=msg.getQueue().getId();
		MessageClient client = getMessageClientByQueueId(queueId);
		if(client==null){
			return 0;
		}
		String msgId = msg.getMessageId();
		if(msgId==null || msgId.trim().length()==0){
			msg.buildMessageId();
		}
		DelCommand command = new DelCommand(msg);
		command.setSessionId(session.getId());
		MQResponse response =client.writeMessage(command);
		if(response==null){
			throw new ConnectionException("not connect to broker server");
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			updateSession(client);
			return response.getIntAttribute(MessageMapping.RESULT);
		}
		
		updateSession(client);
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}
	
	public void updateSession()throws Exception{
		MessageClient client = clientService.getBroker();
		updateSession(client);
	}
	
	public String getSessionId(){
		if(session==null){
			return null;
		}
		return session.getId();
	}

}
