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

package com.dinginfo.seamq.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.common.KetamaNodeLocator;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;

public class QueueService {
	public static final String BEAN_NAME = "queueService";
	
	public final long OBJECT_TIMEOUT= 2 * 60 * 1000;
	
	public final long TIMEOUT_TEN_MINUTE= 10 * 60 * 1000;
	
	public final int TIMEOUT_CACHE= 30 * 60 * 1000;
	
	private CachedMap<String, MQueueAgent> queueAgentMap;
	
	private TopicService topicService;
	
	private QueueAgentBuilder queueAgentBuilder ;
	
	private CachedMap<String,MQueue> queueMap ;
	
	private CachedMap<String,TopicPermission> permissionMap;
	
	private CachedMap<String,List<String>> congroupNameMap;
	
	private String currentNodeId ;
	
	private KetamaNodeLocator ketamaNodeLocator ;
	
	public QueueService(){
		this.queueAgentMap = new CachedMap<String, MQueueAgent>(100000);
		this.queueMap = new CachedMap<String, MQueue>(50000,TIMEOUT_CACHE);
		this.permissionMap = new CachedMap<String, TopicPermission>(100000,TIMEOUT_CACHE);
		this.congroupNameMap = new CachedMap<String,List<String>>(100000, TIMEOUT_CACHE);
	}
	
	public void setQueueAgentBuilder(QueueAgentBuilder queueAgentBuilder) {
		this.queueAgentBuilder = queueAgentBuilder;
	}

	public void setTopicService(TopicService topicService) {
		this.topicService = topicService;
	}

	
	public void setCurrentNodeId(String currentNodeId) {
		this.currentNodeId = currentNodeId;
	}

	public void setKetamaNodeLocator(KetamaNodeLocator ketamaNodeLocator) {
		this.ketamaNodeLocator = ketamaNodeLocator;
	}
	
	public List<String> getConsumerGroupNameList(String topicId) throws Exception{
		List<String> groupNameList =congroupNameMap.get(topicId);
		if(groupNameList!=null) {
			return groupNameList;
		}
		List<ConsumerGroup> groupList = topicService.getConsumerGroupList(topicId);
		if(groupList==null || groupList.size()==0) {
			return null;
		}
		groupNameList = new ArrayList<String>();
		for(ConsumerGroup group : groupList) {
			groupNameList.add(group.getName());
		}
		congroupNameMap.put(topicId, groupNameList);
		return groupNameList;
	}

	public MQueueAgent getQueueAgent(MQueue queue)throws Exception{
		if(queue==null){
			return null;
		}
		String queueId = queue.getId();
		if(queueId ==null || queueId.trim().length()==0){
			queueId = queue.buildQueueId();
		}
		MQueueAgent queueAgent = queueAgentMap.get(queueId);
		if(queueAgent!=null){
			return queueAgent;
		}
		queueAgent = buildQueueAgent(queueId);
		if(queueAgent==null) {
			return null;
		}
		boolean loaded = queueAgent.load(queueId);
		if(loaded) {
			return queueAgent;
		}else {
			queueAgentMap.remove(queueId);
			return null;
		}
	}
	
	private synchronized MQueueAgent buildQueueAgent(String queueId)throws Exception{
		if(queueId==null || queueId.trim().length()==0){
			return null;
		}
		MQueueAgent queueAgent = queueAgentMap.get(queueId);
		if(queueAgent!=null){
			return queueAgent;
		}
		queueAgent = queueAgentBuilder.build();
		queueAgentMap.put(queueId, queueAgent);
		return queueAgent;
	}
	
	
	public boolean checkInPermission(User user,String topicId)throws Exception{
		TopicPermission permission = permissionMap.get(topicId);
		if(permission!=null && !permission.isInTimeout(OBJECT_TIMEOUT)) {
			return permission.checkInPermission(user.getName());
		}
		if(permission==null) {
			permission = new TopicPermission();
			permissionMap.put(topicId, permission);
		}
		List<User> userList = topicService.getProducerUserList(topicId);
		permission.updateProducerPermission(userList);
		return permission.checkInPermission(user.getName());
	}
	
	public boolean checkOutPermission(User user,ConsumerGroup group)throws Exception{
		if(group==null || group.getTopic()==null) {
			return false;
		}
		String topicId = group.getTopic().getId();
		TopicPermission permission = permissionMap.get(topicId);
		if(permission!=null && !permission.isOutTimeout(OBJECT_TIMEOUT)) {
			return permission.checkOutPermission(user,group.getName());
		}
		if(permission==null) {
			permission = new TopicPermission();
			permissionMap.put(topicId, permission);
		}
		List<ConsumerGroup> groupList = getOutUserList(topicId);
		if(groupList==null) {
			return false;
		}
		permission.updateConsumerGroupPermission(groupList);
		return permission.checkOutPermission(user, group.getName());
	}

	
	private List<ConsumerGroup> getOutUserList(String topicId) throws Exception{
		List<User> userList = topicService.getConsumerUserList(topicId);
		if(userList==null) {
			return null;
		}
		Map<String,ConsumerGroup> groupMap = new HashMap<String,ConsumerGroup>();
		ConsumerGroup cgroup = null;
		for(User user : userList) {
			cgroup = groupMap.get(user.getGroupName());
			if(cgroup==null) {
				cgroup = new ConsumerGroup();
				cgroup.setName(user.getGroupName());
				groupMap.put(user.getGroupName(), cgroup);
			}
			cgroup.putUser(user);
		}
		List<ConsumerGroup> groupList = new ArrayList<ConsumerGroup>();
		for(ConsumerGroup cg : groupMap.values()) {
			groupList.add(cg);
		}
		return groupList;
		
	}
	
	public void rebuildQueue(){
		
	}
	public boolean checkPermission(User user,MQueue queue)throws Exception{
		MQueue q = getQueue(queue);
		if(q==null){
			return false;
		}
		if(user.getDomain()==null || q.getDomain()==null){
			return false;
		}
		if(user.getDomain().getId()==q.getDomain().getId()){
			return true;
		}else{
			return false;
		}
	}
	
	private MQueue getQueue(MQueue queue)throws Exception{
		if(queue.getId()==null){
			queue.buildQueueId();
		}
		MQueue q = queueMap.get(queue.getId());
		if(q!=null){
			return q;
		}
		q = topicService.getQueueByPK(queue);
		if(queue==null){
			return null;
		}
		queueMap.put(q.getId(), q);
		return q;
	}
	
	public void removeQueueStatus(String queueId){
		queueAgentMap.remove(queueId);
	}
	
	public void clear(){
		queueAgentMap.clear();
	}
	
	class TopicPermission{
		private String topicId;
		
		private long inTime;
		
		private long outTime;
		
		private Set<String> producerUserSet;
		
		private Map<String,Set<String>> congroupUserMap;
		
		
		
		public long getInTime() {
			return inTime;
		}

		public void setInTime(long inTime) {
			this.inTime = inTime;
		}

		public long getOutTime() {
			return outTime;
		}

		public void setOutTime(long outTime) {
			this.outTime = outTime;
		}

		public void updateProducerPermission(List<User> userList) {
			if(userList==null) {
				return;
			}
			Set<String> userSet = new HashSet<String>();
			for(User user : userList) {
				userSet.add(user.getName());			
			}
			this.producerUserSet = userSet;
			this.inTime = System.currentTimeMillis();
		}
		
		public boolean checkInPermission(String userId) {
			if(producerUserSet==null) {
				return false;
			}
			return producerUserSet.contains(userId);
		}
		
		public void updateConsumerGroupPermission(List<ConsumerGroup> groupList) {
			if(groupList==null) {
				return;
			}
			Map<String,Set<String>> userMap = new HashMap<String,Set<String>>();
			Set<String> userSet = null;
			List<User> userList = null;
			for(ConsumerGroup group : groupList) {
				userList = group.getUserList();
				if(userList!=null) {
					userSet = new HashSet<String>();
					for(User user : userList) {
						userSet.add(user.getName());
					}
					userMap.put(group.getName(), userSet);
				}
			}
			this.congroupUserMap = userMap;
			this.outTime = System.currentTimeMillis();
		}
		
		public boolean checkOutPermission(User user,String groupName) {
			if(congroupUserMap==null || user==null || groupName==null) {
				return false;
			}
			Set<String> userSet = congroupUserMap.get(groupName);
			if(userSet==null) {
				return false;
			}
			String username = user.getName();
			return userSet.contains(username);
		}
		
		public boolean checkOutPermission(User user) {
			if(congroupUserMap==null || user==null) {
				return false;
			}
			for(Set<String> userSet : congroupUserMap.values()) {
				if(userSet.contains(user.getName())) {
					return true;
				}
			}
			return false;
		}
		
		public boolean isInTimeout(long timeout) {
			long time = System.currentTimeMillis() - inTime;
			if(time>=timeout) {
				return true;
			}else {
				return false;
			}
		}
		
		public boolean isOutTimeout(long timeout) {
			long time = System.currentTimeMillis() - outTime;
			if(time>=timeout) {
				return true;
			}else {
				return false;
			}
		}
	}
	
}
