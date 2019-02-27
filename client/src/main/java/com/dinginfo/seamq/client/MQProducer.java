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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.MQMessage;
import com.dinginfo.seamq.MQMessageArray;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.PutCommand;
import com.dinginfo.seamq.client.exception.ConnectionException;
import com.dinginfo.seamq.client.exception.OutOfMaxMessageSize;
import com.dinginfo.seamq.client.exception.TimeoutException;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.mapping.MessageMapping;

public class MQProducer extends MQClient{
	public static final int MAX_MESSAGE_SIZE=1024 * 1024 * 8;
	
	private static final Logger logger = LogManager.getLogger(MQProducer.class);
	
	
	protected MQProducer(MQClientService service) {
		this.clientService = service;
	}

	public String send(MQMessage message) throws Exception {

		checkMessage(message);
		
		MQTopic t = new MQTopic(domainId, message.getTopicName());
		String topicId = t.buildTopicId();
		MQTopic topic = clientService.getTopicInfo(domainId,topicId);
		if (topic == null) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("not topic :");
			sb.append(message.getTopicName());
			throw new Exception(sb.toString());
		}
		if(MQTopic.STATUS_DISABLE.equals(topic.getStatus())){
			StringBuilder sb = new StringBuilder(100);
			sb.append("topic:");
			sb.append(message.getTopicName());
			sb.append(" is disable");
			throw new Exception(sb.toString());
		}

		
		// get a connected client
		int queueIndex =0;
		MQueue q =null;
		String queueId =null;
		MessageClient client=null;
		int connectionRepeatNum = topic.getQueueNum();
		if(connectionRepeatNum>clientService.getConnectionRepeatNum()){
			connectionRepeatNum = clientService.getConnectionRepeatNum();
		}
		for(int i=0;i<connectionRepeatNum;i++){
			queueIndex = getQueueIndex(topic);
			q = new MQueue(topic.getId(),queueIndex);
			queueId = q.getId();
			client = getMessageClientByQueueId(queueId);
			if(client.isContected()){
				break;
			}
		}
		if(!client.isContected()){
			return null;
		}
		MQMessageExt msg = new MQMessageExt(message);
		t = new MQTopic();
		t.setId(topicId);
		msg.setTopic(t);
		MQueue queue = new MQueue();
		queue.setId(queueId);
		queue.setQueue(queueIndex);
		msg.setQueue(queue);
		if(message instanceof MQMessageExt){
			msg.setArraySize(((MQMessageExt)message).getArraySize());
		}
		String messageId = null;
		MessageResult result = null;
		int messageRepeatNum = clientService.getMessageRepeatNum();
		Map<String,String> disableMap = null;
		String queueName = null;
		for(int i=0;i<messageRepeatNum;i++){
			result=sendMessage(client,msg);
			if(result==null){
				continue;
			}

			if(ResponseStatus.STATUS_SUCCESS.equals(result.getStatus())){
				messageId = result.getMessageId();
				break;
			}
			if(ResponseStatus.STATUS_DISABLE.equals(result.getStatus())){
				if(disableMap==null){
					disableMap = new HashMap<String, String>();
				}
				queueName = String.valueOf(msg.getQueue().getQueue());
				disableMap.put(queueName, queueName);
				int n = topic.getQueueNum();
				for(int j=0;j<n;j++){
					queueIndex = getQueueIndex(topic);
					if(!disableMap.containsKey(String.valueOf(queueIndex))){
						break;
					}
				}
				q = new MQueue(topic.getId(),queueIndex);
				queueId = q.getId();
				queue = new MQueue();
				queue.setId(queueId);
				queue.setQueue(queueIndex);
				msg.setQueue(queue);
				result = sendMessage(client, msg);
				if(ResponseStatus.STATUS_SUCCESS.equals(result.getStatus())){
					messageId = result.getMessageId();
					break;
				}
			}
		}
		updateSession();
		return messageId;
	}
	
	public String send(List<MQMessage> messageList)throws Exception{
		if(messageList == null || messageList.size()==0){
			throw new Exception("Message array is null");
		}
		MQMessage msg = messageList.get(0);
		String topicName = msg.getTopicName();
		if(topicName == null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQMessageArray msgArray = new MQMessageArray();
		for(MQMessage message : messageList){
			if(!topicName.equals(message.getTopicName())){
				throw new Exception("all message must have same topic name");
			}
			msgArray.put(message);
		}
		MQMessageExt message = new MQMessageExt();
		message.setArraySize(messageList.size());
		message.setTopicName(topicName);
		message.setData(msgArray.toBytes());
		return send(message);
	}
	
	private void sleep(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
		}
	}
	
	private int getQueueIndex(MQTopic topic){
		int queue = 0;
		int queueNum = topic.getQueueNum();
		if (queueNum > 1) {
			Random random = new Random();
			queue = random.nextInt(queueNum);
			if(queue<0){
				queue= Math.abs(queue);
			}
		}
		return queue;
	}
	
	private int getQueueNum(String topicName)throws Exception{
		MQTopic t = new MQTopic(domainId,topicName);
		MQTopic topic =clientService.getTopicInfo(domainId,t.buildTopicId());
		if(topic!=null){
			return topic.getQueueNum();
		}else{
			return 0;	
		}
	}

	

	public String send(MQMessage message, int queueIndex)
			throws Exception {
		checkMessage(message);

		if(queueIndex<0){
			StringBuilder sb = new StringBuilder(100);
			sb.append("queue is invalid,");
			sb.append("topic queue is :");
			sb.append(queueIndex);
			throw new Exception(sb.toString());
		}
		MQTopic t = new MQTopic(domainId,message.getTopicName());
		String topicId = t.buildTopicId();

		MQTopic topic = clientService.getTopicInfo(domainId,topicId);
		if(topic==null){
			StringBuilder sb = new StringBuilder(100);
			sb.append("not topic :");
			sb.append(message.getTopicName());
			throw new Exception(sb.toString());
		}
		if(MQTopic.STATUS_DISABLE.equals(topic.getStatus())){
			StringBuilder sb = new StringBuilder(100);
			sb.append("topic:");
			sb.append(message.getTopicName());
			sb.append(" is disable");
			throw new Exception(sb.toString());
		}
		if(queueIndex>=topic.getQueueNum()){
			StringBuilder sb = new StringBuilder(100);
			sb.append("queue is invalid,");
			sb.append("topic queue is :");
			sb.append(topic.getQueueNum());
			throw new Exception(sb.toString());
		}
		
		MQueue queue = new MQueue(topic.getId(),queueIndex);
		if(queue.getId()==null){
			queue.buildQueueId();
		}
		MessageClient client =null;
		int connectionRepeatNum =clientService.getConnectionRepeatNum();
		for(int i=0;i<connectionRepeatNum;i++){
			client = getMessageClientByQueueId(queue.getId());
			if(client.isContected()){
				break;
			}else{
				sleep(SLEEP_TIME);
			}
		}
		if(!client.isContected()){
			return null;
		}
		MQMessageExt msg = new MQMessageExt(message);
		MQueue q = new MQueue();
		q.setId(queue.getId());
		msg.setQueue(q);
		String messageId =null;
		MessageResult result = null;
		int messageRepeatNum = clientService.getConnectionRepeatNum();
		for(int i=0;i<messageRepeatNum;i++){
			//send messge
			result = sendMessage(client,msg);
			if(result==null){
				continue;
			}
			messageId = result.getMessageId();
			if(messageId!=null && messageId.trim().length()>0){
				break;
			}
		}
		updateSession();
		return messageId;
	}
	
	public String send(List<MQMessage> messageList, int queueIndex) throws Exception{
		if(messageList == null || messageList.size()==0){
			throw new Exception("Message array is null");
		}
		MQMessage msg = messageList.get(0);
		String topicName = msg.getTopicName();
		if(topicName == null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQMessageArray msgArray = new MQMessageArray();
		for(MQMessage message : messageList){
			if(!topicName.equals(message.getTopicName())){
				throw new Exception("all message must have same topic name");
			}
			msgArray.put(message);
		}
		MQMessageExt message = new MQMessageExt();
		message.setArraySize(messageList.size());
		message.setTopicName(topicName);
		message.setData(msgArray.toBytes());
		return send(message,queueIndex);
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

	private void checkMessage(MQMessage message) throws Exception {
		if (message == null || message.getTopicName() == null) {
			throw new Exception("topic name is null");
		}
		if (message.getData() == null || message.getData().length == 0) {
			throw new Exception("message data is null");
		}
		int messageSize = message.getData().length;
		List<DataField> attList = message.getAttributeList();
		if(attList!=null){
			for(DataField att : attList){
				messageSize = messageSize + att.getSize();
			} 
		}
		if(messageSize >MAX_MESSAGE_SIZE){
			StringBuilder sb = new StringBuilder();
			sb.append("message max size is 8MB,current message size is:");
			if(messageSize>(1024*1024)){
				int m = messageSize /(1024*1024);
				sb.append(String.valueOf(m));
				sb.append("MB");
			}else if(messageSize>1024){
				int m = messageSize /1024;
				sb.append(String.valueOf(m));
				sb.append("KB");
			}else{
				sb.append(String.valueOf(messageSize));
			}
			throw new OutOfMaxMessageSize(sb.toString());
		}
	}

	private MessageResult sendMessage(MessageClient client, MQMessageExt msg)
			throws Exception {
		ClientCommand command = new PutCommand(msg);
		command.setSessionId(session.getId());
		MQResponse response =client.writeMessage(command);
		if(response==null){
			throw new ConnectionException("not connect to broker server");
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			
			String messageId = response.getStringField(MessageMapping.FIELD_MESSAGE_ID);
			MessageResult result = new MessageResult();
			result.setStatus(response.getStatus());
			result.setMessageId(messageId);
			return result;	
		}
		
		if(ResponseStatus.STATUS_DISABLE.equals(response.getStatus())){
			MessageResult result = new MessageResult();
			result.setStatus(response.getStatus());
			result.setMessageId(null);
			return result;	
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
	
	class MessageResult {
		private String status = null;
		private String messageId = null;
		public String getStatus() {
			return status;
		}
		public void setStatus(String status) {
			this.status = status;
		}
		public String getMessageId() {
			return messageId;
		}
		public void setMessageId(String messageId) {
			this.messageId = messageId;
		}
		
	}
}
