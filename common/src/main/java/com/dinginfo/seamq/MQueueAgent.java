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

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;

public abstract class MQueueAgent implements Serializable {
	private static final int GET_MAX = 100;
	
	protected long sessionTime= 10 * 1000;
	
	protected MQDomain domain;
	
	protected MQTopic topic;
	
	protected MQueue queue;
	
	protected long inOffset;

	protected long outOffset;
	
	protected String status;
	
	protected  ReentrantReadWriteLock inLock =null;
	
	protected  ReentrantReadWriteLock outLock = null;
	
	protected Date createdTime;
	
	protected boolean loaded;
	
	protected Map<String,OutPosition> outPositionMap;

	public MQueueAgent() {
		this.inOffset = 0;
		this.outOffset = 0;
		this.outPositionMap = new HashMap<String,OutPosition>();
		this.inLock = new ReentrantReadWriteLock();
		this.outLock = new ReentrantReadWriteLock();
	}
	
	public MQMessageExt get(MQMessageExt message) throws Exception{
		String congroupName = message.getConsumerGroupName();
		OutPosition position = outPositionMap.get(congroupName);
		if(position==null) {
			position = loadOutPositionByPK(queue.getId(), congroupName);
			if(position==null) {
				return null;
			}else {
				outPositionMap.put(position.getGroupName(), position);
			}
		}
		
		if(position.getOutOffset()>=inOffset) {
			return null;
		}
		
		if(!checkSession(message, position)) {
			return null;
		}
		
		try {
			outLock.writeLock().lock();
			if(position.getSessionId()==null) {
				position.setSessionId(message.getSessionId());
			}
		}finally {
			outLock.writeLock().unlock();
		}
		
		if(position.getSessionId()!=null && !position.getSessionId().equals(message.getSessionId())) {
			return null;
		}
		MQMessageExt msg = null;
		try {
			msg = get(message, position);
		}catch(Exception e) {
			position.setSessionId(null);
			throw e;
		}
		return msg;
	}
	
	private MQMessageExt get(MQMessageExt message,OutPosition position)throws Exception{
		MQMessageExt msg = null;
		MQueue queue = null;
		OutPosition positionBean= null;
		for(int i=0;i<GET_MAX;i++) {
			message.setOffset(position.getOutOffset());
			message.buildMessageId();
			msg =getFromStorage(message);
			if(msg!=null) {
				msg.setQueue(message.getQueue());
				msg.setOffset(position.getOutOffset());
				msg.setMessageId(message.getMessageId());
				position.setMessageId(msg.getMessageId());
				Date updatedTime = new Date(System.currentTimeMillis());
				position.setUpdateTime(updatedTime);
				return msg;
			}else {
				queue = loadQueueByPK(message.getQueue().getId());
				if(queue!=null){
					this.inOffset = queue.getInOffset();
					this.outOffset = queue.getOutOffset();
				}
				positionBean = loadOutPositionByPK(queue.getId(), message.getConsumerGroupName());
				if(positionBean==null) {
					break;
				}
				if(positionBean.getOutOffset()>=inOffset) {
					break;
				}
				positionBean = new OutPosition();
				positionBean.setId(position.getId());
				positionBean.setOutOffset(position.getOutOffset());
				if(increaseOutPosition(positionBean)>0){
					positionBean.setMessageId(message.getMessageId());
					position.setOutOffset(position.getOutOffset()+1);
					position.setSessionId(null);
				}
			}
		}
		return null;
	}
	
	
	private boolean checkSession(MQMessageExt message, OutPosition position) {
		String sid =message.getSessionId();
		String sessionId = position.getSessionId();
		if(sessionId==null || sessionId.equals(sid)) {
			return true;
		}
		Date updatedTime = position.getUpdateTime();
		long time = System.currentTimeMillis() - updatedTime.getTime();
		if(time>sessionTime){
			return true;
		}else {
			return false;
		}
	}
	
	public int delete(MQMessageExt message) throws Exception{
		String congroupName = message.getConsumerGroupName();
		OutPosition position = outPositionMap.get(congroupName);
		if(position==null) {
			position = loadOutPositionByPK(queue.getId(), congroupName);
			if(position==null) {
				return 0;
			}else {
				outPositionMap.put(position.getGroupName(), position);
			}
		}
		
		if(position.getOutOffset()>=inOffset) {
			return 0;
		}
		
		if(position.getSessionId()==null || position.getSessionId().trim().length()==0){
			return 0;
		}
		String sessionId = message.getSessionId();
		if(!position.getSessionId().equals(sessionId)){
			return 0;
		}
		String curMsgId = position.getMessageId(); 
		if(curMsgId==null || curMsgId.trim().length()==0){
			return 0;
		}
		if(!curMsgId.equals(message.getMessageId())){
			return 0;
		}
		
		OutPosition positionBean = new OutPosition();
		positionBean.setId(position.getId());
		positionBean.setOutOffset(position.getOutOffset());
		if(increaseOutPosition(positionBean)>0) {
			position.setOutOffset(position.getOutOffset()+1);
			position.setSessionId(null);
			return 1;
		}else {
			positionBean =loadOutPositionByPK(queue.getId(), message.getConsumerGroupName());
			if(positionBean!=null) {
				position.setOutOffset(positionBean.getOutOffset());
			}
			return 0;
		}
	}

	public MQMessageExt put(MQMessageExt message) throws Exception {
		try{
			inLock.writeLock().lock();
			message.setOffset(inOffset);
			MessageStatus msgStatus = putToStorage(message);
			if(msgStatus==null){
				return null;
			}
			if(MessageStatus.OK==msgStatus.getStatus()){
				inOffset=inOffset+1;
				return message;
			}else{				
				MQueue queue = loadQueueByPK(message.getQueue().getId());
				if(queue!=null){
					this.inOffset = queue.getInOffset();
					this.outOffset = queue.getOutOffset();
				}
				return null;
			}	
		}catch(Exception e){
			MQueue queue = loadQueueByPK(message.getQueue().getId());
			if(queue!=null){
				this.inOffset = queue.getInOffset();
				this.outOffset = queue.getOutOffset();
			}
			throw e;
		}
		finally{
			inLock.writeLock().unlock();
		}
	}
	
	
	protected abstract MessageStatus putToStorage(MQMessageExt message)throws Exception;
	
	protected abstract MQMessageExt getFromStorage(MQMessageExt message) throws Exception;
	
	protected abstract MQueue loadQueueByPK(String queueId)throws Exception;
	
	protected abstract MQTopic getTopicByPK(String topicId)throws Exception;
	
	protected abstract int deleteFromStroage(MQMessageExt message)throws Exception;
	
	protected abstract OutPosition loadOutPositionByPK(String queueId, String userId)throws Exception;
	
	protected abstract int increaseOutPosition(OutPosition out)throws Exception;
	
	public abstract boolean close() throws Exception;
	
	public synchronized boolean load(String queueId) throws Exception{
		if(loaded) {
			return true;
		}
		this.queue = loadQueueByPK(queueId);
		if(queue==null) {
			loaded = true;
			return false;
		}
		this.inOffset = queue.getInOffset();
		this.outOffset = queue.getOutOffset();
		this.topic = getTopicByPK(queue.getTopic().getId());
		this.domain = queue.getDomain();
		this.loaded = true;
		return true;
	}
	
	
	public long getNotifyOffset(String name) throws Exception {
		OutPosition bean = outPositionMap.get(name);
		if(bean==null) {
			bean = loadOutPositionByPK(queue.getId(), name);
			if(bean==null) {
				return 0;
			}
		}
		return inOffset - bean.getOutOffset();
	}
	
	public MQDomain getDomain() {
		return domain;
	}


	public MQTopic getTopic() {
		return topic;
	}


	public MQueue getQueue() {
		return queue;
	}


	public long getInOffset() {
		return inOffset;
	}


	public long getOutOffset() {
		return outOffset;
	}


	public String getStatus() {
		return status;
	}


	public long getSessionTime() {
		return sessionTime;
	}

	public void setSessionTime(long sessionTime) {
		this.sessionTime = sessionTime;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

}
