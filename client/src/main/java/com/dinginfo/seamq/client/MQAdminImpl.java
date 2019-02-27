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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.QueueCommand;
import com.dinginfo.seamq.client.command.TopicCommand;
import com.dinginfo.seamq.client.command.UserCommand;
import com.dinginfo.seamq.client.exception.ConnectionException;
import com.dinginfo.seamq.client.exception.TimeoutException;
import com.dinginfo.seamq.common.NameUtil;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.QueueMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.entity.mapping.UserMapping;

public class MQAdminImpl extends MQClient implements MQAdmin{
	private static final Logger logger = LogManager.getLogger(MQAdminImpl.class);
	
	private Map<String,String> nameMap =new HashMap<String,String>();
	

	protected MQAdminImpl(MQClientService service) {
		this.clientService = service;
	}


	public void createUser(User user) throws Exception {
		updateSession();
		createUser(MQDomain.DEFAULT_DOMAIN_ID, user);
	}
	

	private void createUser(long domainId, User user) throws Exception {
		if(user==null || user.getName()==null){
			throw new Exception("user is null");
		}
		if(!NameUtil.check(user.getName())){
			throw new Exception("invalid user name");
		}
		user.setDomain(new MQDomain(domainId));
		user.setName(user.getName().toLowerCase());
		ClientCommand command = new UserCommand(UserCommand.ACTION_CREATE,user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.length()>0){
			throw new Exception(response.getException());
		}
		
	}

	public int updateUser(User user) throws Exception {
		updateSession();
		return updateUser(MQDomain.DEFAULT_DOMAIN_ID, user);
	}

	
	public int updateUser(long domainId,User user) throws Exception {
		if(user==null || user.getName()==null){
			throw new Exception("user is null");
		}
		int result = 0;
		user.setDomain(new MQDomain(domainId));
		user.setName(user.getName().toLowerCase());
		ClientCommand command = new UserCommand(UserCommand.ACTION_UPDATE, user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			result = response.getIntAttribute(UserMapping.RESULT);
			return result;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptinStr = response.getException();
		if(exceptinStr!=null && exceptinStr.trim().length()>0){
			throw new Exception(exceptinStr);
		}
		
		return result;
	}
	

	public int updateUserPassword(User user) throws Exception {
		updateSession();
		return updateUserPassword(MQDomain.DEFAULT_DOMAIN_ID, user);
	}
	
	
	private int updateUserPassword(long domainId,User user)
			throws Exception {
		if(user==null || user.getName()==null){
			throw new Exception("user is null");
		}
		int result = 0;
		
		user.setDomain(new MQDomain(domainId));
		user.setName(user.getName().toLowerCase());
		ClientCommand command = new UserCommand(UserCommand.ACTION_UPDATE_PASSWD,user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			result = response.getIntAttribute(UserMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionString = response.getException();
		if(exceptionString!=null && exceptionString.trim().length()>0){
			throw new Exception(exceptionString);
		}
		
		return result;
	}

	public int deleteUser(String userId) throws Exception { 
		updateSession();
		return deleteUser(MQDomain.DEFAULT_DOMAIN_ID, userId);
	}


	private int deleteUser(long domainId,String userId) throws Exception {
		if(userId==null){
			throw new Exception("user id is null");
		}
		User user = new User();
		user.setName(userId.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		ClientCommand command = new UserCommand(UserCommand.ACTION_DELETE,user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			int result = response.getIntAttribute(UserMapping.RESULT);
			return result;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}

	
	public User getUserByPK(String userId) throws Exception { 
		updateSession();
		return getUserByPK(MQDomain.DEFAULT_DOMAIN_ID, userId);
	}

	private User getUserByPK(long domainId,String userId) throws Exception {
		if(userId==null){
			throw new Exception("user id is null");
		}
		User user = new User();
		user.setName(userId.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		ClientCommand command = new UserCommand(UserCommand.ACTION_GET,user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			String username =response.getStringAttribute(UserMapping.FIELD_NAME);
			if(username==null) {
				return null;
			}
			user = new User();
			user.setName(username);
			user.setStatus(response.getStringAttribute(UserMapping.FIELD_STATUS));
			user.setRemark(response.getStringAttribute(UserMapping.FIELD_REMARK));
			user.setType(response.getStringAttribute(UserMapping.FIELD_TYPE));
			user.setDomain(new MQDomain(response.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID)));
			long time=response.getLongAttribute(UserMapping.FIELD_CREATED_TIME);
			if(time>0){
				user.setCreatedTime(new Date(time));
			}
			time = response.getLongAttribute(UserMapping.FIELD_UPDATED_TIME);
			if(time>0){
				user.setUpdatedTime(new Date(time));
			}
			return user;
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

	@Override
	public long getUserCount() throws Exception {
		updateSession();
		return getUserCount(MQDomain.DEFAULT_DOMAIN_ID);
	}
	
	private long getUserCount(long domainId) throws Exception {
		User user = new User();
		user.setDomain(new MQDomain(domainId));
		ClientCommand command = new UserCommand(UserCommand.ACTION_COUNT, user);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			long result = response.getLongAttribute(UserMapping.RESULT);
			return result;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}


	@Override
	public List<User> getUserList(int pageSize, int pageNo) throws Exception {
		updateSession();
		return getUserList(MQDomain.DEFAULT_DOMAIN_ID, pageSize, pageNo);
	}
	
	private List<User> getUserList(long domainId, int pageSize, int pageNo) throws Exception {
		if(pageSize>1000){
			throw new Exception("max page size is 1000");
		}
		if(pageNo<0){
			throw new Exception("pageNo is not right");
		}
		User user = new User();
		user.setDomain(new MQDomain(domainId));
		UserCommand command = new UserCommand(UserCommand.ACTION_LIST, user);
		command.setSessionId(session.getId());
		command.setPageSize(pageSize);
		command.setPageNo(pageNo);
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			byte[] data = response.getData();
			if(data!=null && data.length>0) {
				String jsonString = new String(data);
				List<User> userList = JSON.parseArray(jsonString, User.class);
				return userList;
			}
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.length()>0){
			throw new Exception(exceptionStr);
		}
		return null;
	}


	@Override
	public List<User> getUserListByTopic(String topicName) throws Exception {
		updateSession();
		return getUserListByTopic(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private List<User> getUserListByTopic(long domainId,String topicName) throws Exception{
		if(topicName == null){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic(domainId,topicName);
		
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_USER_LIST, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			byte[] data = response.getData();
			if(data!=null && data.length>0) {
				String jsonString = new String(data);
				return JSON.parseArray(jsonString, User.class);	
			}
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


	public void createTopic(MQTopic topic) throws Exception {
		updateSession();
		createTopic(MQDomain.DEFAULT_DOMAIN_ID,topic);
	}

	
	private void createTopic(long domainId, MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		if(!NameUtil.check(topic.getName())){
			throw new Exception("topic name is invalid");
		}
		if(topic.getQueueNum()<1){
			throw new Exception("topic queue number is invalid");
		}
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_CREATE,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
	}


	@Override
	public int updateTopic(MQTopic topic) throws Exception {
		updateSession();
		return updateTopic(MQDomain.DEFAULT_DOMAIN_ID, topic);
	}
	
	private int updateTopic(long domainId,MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_UPDATE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			int n = response.getIntAttribute(TopicMapping.RESULT);
			return n;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}


	public int dropTopic(String topicName) throws Exception {
		updateSession();
		return dropTopic(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}

	
	private int dropTopic(long domainId, String topicName)
			throws Exception {
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_DROP,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		
		
		return 0;
	}

	public MQTopic getTopic(String topicName)throws Exception{
		updateSession();
		return getTopic(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private MQTopic getTopic(long domainId,String topicName)throws Exception{
		
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GET, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			topic = new MQTopic();
			topic.setName(response.getStringAttribute(TopicMapping.FIELD_NAME));
			topic.setId(response.getStringAttribute(TopicMapping.FIELD_ID));
			topic.setStatus(response.getStringAttribute(TopicMapping.FIELD_STSTUS));
			topic.setQueueNum(response.getIntAttribute(TopicMapping.FIELD_QUEUE_NUM));
			topic.setDomain(new MQDomain(response.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID)));
			topic.setRemark(response.getStringAttribute(TopicMapping.FIELD_REMARK));
			return topic;
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
	
	@Override
	public long getTopicCount() throws Exception {
		updateSession();
		return getTopicCount(MQDomain.DEFAULT_DOMAIN_ID);
	}

	private long getTopicCount(long domainId) throws Exception {
		MQTopic topic = new MQTopic();
		topic.setDomain(new MQDomain(domainId));
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_COUNT, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getLongAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}

	public List<MQTopic> getTopicList(int pageSize,int pageNo)throws Exception{
		updateSession();
		return getTopicList(MQDomain.DEFAULT_DOMAIN_ID, pageSize, pageNo);
	}
	
	private List<MQTopic> getTopicList(long domainId,int pageSize,int pageNo)throws Exception{
		if(pageSize>10000){
			throw new Exception("max page size is 10000");
		}
		if(pageNo<0){
			throw new Exception("pageNo is not right");
		}
		MQTopic topic = new MQTopic();
		topic.setDomain(new MQDomain(domainId));
		TopicCommand command = new TopicCommand(TopicCommand.ACTION_LIST, topic);
		command.setSessionId(session.getId());
		command.setPageSize(pageSize);
		command.setPageNo(pageNo);
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			List<MQTopic> topicList = new ArrayList<MQTopic>();
			byte[] data = response.getData();
			if(data!=null && data.length>0) {
				String jsonString = new String(data);
				topicList = JSON.parseArray(jsonString, MQTopic.class);
			}
			return topicList;
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

	public int disableTopic(String topicName) throws Exception {
		updateSession();
		return disableTopic(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private int disableTopic(long domainId,String topicName)throws Exception{
		
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_DISABLE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}

	public int enableTopic(String topicName) throws Exception {
		updateSession();
		return enableTopic(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private int enableTopic(long domainId,String topicName)throws Exception{
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_ENABLE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}
	
	public int disableQueue(String topicName) throws Exception {
		updateSession();
		return disableQueue(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private int disableQueue(long domainId,String topicName)throws Exception{
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_DISABLE_QUEUE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}
	
	public int enableQueue(String topicName) throws Exception {
		updateSession();
		return enableQueue(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private int enableQueue(long domainId,String topicName)throws Exception{
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_ENABLE_QUEUE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}


	public int increaseQueue(String topicName) throws Exception {
		updateSession();
		return increaseQueue(MQDomain.DEFAULT_DOMAIN_ID, topicName, 1);
	}

	
	public int increaseQueue(String topicName, int qty) throws Exception {
		updateSession();
		return increaseQueue(MQDomain.DEFAULT_DOMAIN_ID, topicName, qty);
	}
	
	private int increaseQueue(long domainId,String topicName, int qty) throws Exception {
		if(qty<1){
			throw new Exception("Qty is invalid");
		}
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		topic.setQueueNum(qty);
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_INCREASE_QUEUE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}


	public int reduceQueue(String topicName) throws Exception {
		updateSession();
		return reduceQueue(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private int reduceQueue(long domainId,String topicName)throws Exception{
		MQTopic topic = new MQTopic(domainId,topicName);
		topic.buildTopicId();
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_REDUCE_QUEUE, topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}

	public MQueue getQueueInfo(String topicName,int queueIndex)throws Exception{
		updateSession();
		return getQueueInfo(MQDomain.DEFAULT_DOMAIN_ID, topicName, queueIndex);
	}
	
	private MQueue getQueueInfo(long domainId,String topicName,int queueIndex)throws Exception{
		MQTopic topic = getTopic(domainId, topicName);
		if(topic==null){
			StringBuilder sb = new StringBuilder(100);
			sb.append("topic ");
			sb.append(topicName);
			sb.append(" is null");
			throw new Exception(sb.toString());
		}
		MQueue queue = new MQueue(topic.getId(),queueIndex);
		ClientCommand command = new QueueCommand(QueueCommand.ACTION_GET,queue);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			String queueId = response.getStringAttribute(QueueMapping.FIELD_QUEUE_ID);
			if(queueId==null || queueId.trim().length()==0){
				return null;
			}
			queue = new MQueue(queueId);
			queue.setDomain(new MQDomain(response.getLongAttribute(QueueMapping.FIELD_DOMAIN_ID)));
			MQTopic bean = new MQTopic();
			bean.setId(response.getStringAttribute(QueueMapping.FIELD_TOPIC_ID));
			queue.setTopic(bean);
			queue.setStatus(response.getStringAttribute(QueueMapping.FIELD_STATUS));
			queue.setInOffset(response.getLongAttribute(QueueMapping.FIELD_IN_OFFSET));
			queue.setOutOffset(response.getLongAttribute(QueueMapping.FIELD_OUT_OFFSET));
			return queue; 
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		if(response.getException()!=null && response.getException().trim().length()>0){
			throw new Exception(response.getException());
		}
		return null;
		
	}
	
	public List<MQueue> getQueueList(String topicName,int pageSize,int pageNo)throws Exception{
		updateSession();
		return getQueueList(MQDomain.DEFAULT_DOMAIN_ID, topicName, pageSize, pageNo);
	}
	
	private List<MQueue> getQueueList(long domainId,String topicName,int pageSize,int pageNo)throws Exception{
		if(pageSize>10000){
			throw new Exception("max page size is 10000");
		}
		if(pageNo<0){
			throw new Exception("pageNo is not right");
		}
		MQTopic topic = getTopic(domainId, topicName);
		if(topic==null){
			StringBuilder sb = new StringBuilder(100);
			sb.append("topic ");
			sb.append(topicName);
			sb.append(" is null");
			throw new Exception(sb.toString());
		}
		String topicId=topic.getId();
		MQueue queue = new MQueue(topicId,0);
		QueueCommand command = new QueueCommand(QueueCommand.ACTION_GET_LIST, queue);
		command.setSessionId(session.getId());
		command.setPageSize(pageSize);
		command.setPageNo(pageNo);
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.endsWith(response.getStatus())){
			List<MQueue> queueList = new ArrayList<MQueue>();
			byte[] data = response.getData();
			if(data!=null && data.length>0) {
				String jsonString = new String(data);
				queueList = JSON.parseArray(jsonString, MQueue.class);
			}
			return queueList;
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
	
	public int grantUserToProducer(String topicName, String userName) throws Exception {
		updateSession();
		return grantUserToProducer(MQDomain.DEFAULT_DOMAIN_ID, topicName, userName);
	}
	
	private int grantUserToProducer(long domainId,String topicName,String userName)
			throws Exception {
		if(userName==null || userName.trim().length()==0){
			throw new Exception("user is null");
		}
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		User user = new User(userName.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		topic.setUser(user);
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GRANT_USER_TO_PRODUCER,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		
		return 0;
	}
	
	@Override
	public int removeProducerUser(String topicName, String userName) throws Exception {
		updateSession();
		return removeUserFromProducer(MQDomain.DEFAULT_DOMAIN_ID,topicName,userName);
	}
	
	private int removeUserFromProducer(long domainId,String topicName,String userName)
			throws Exception {
		if(userName==null || userName.trim().length()==0){
			throw new Exception("user is null");
		}
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		User user = new User(userName.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		topic.setUser(user);
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_REMOVE_PRODUCER_USER,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		
		return 0;
	}
	

	@Override
	public List<User> getProducerUserList(String topicName) throws Exception {
		updateSession();
		return getUserListFromProducer(MQDomain.DEFAULT_DOMAIN_ID,topicName);
	}
	
	private List<User> getUserListFromProducer(long domainId,String topicName) throws Exception {
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GET_PRODUCER_USER_LIST,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			byte[] data = response.getData();
			if(data==null || data.length==0) {
				return null;
			}
			String jsonString = new String(data);
			return JSON.parseArray(jsonString, User.class);
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
	
	@Override
	public void createConsumerGroup(String topicName, String groupName) throws Exception {
		updateSession();
		createConsumerGroup(MQDomain.DEFAULT_DOMAIN_ID, topicName, groupName);
	}
	
	public void createConsumerGroup(long domainId,String topicName, String groupName) throws Exception {
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(groupName==null || groupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		if(ConsumerGroup.DEFAULT_GROUP_NAME.equals(groupName.toLowerCase())) {
			throw new Exception("consumer group name is default");
		}
		
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		ConsumerGroup group = new ConsumerGroup();
		group.setName(groupName);
		topic.setConsumerGroup(group);
		
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_CREATE_CONSUMER_GROUP,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return;
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
	}


	@Override
	public int dropConsumerGroup(String topicName, String groupName) throws Exception {
		updateSession();
		return dropConsumerGroup(MQDomain.DEFAULT_DOMAIN_ID, topicName, groupName);
	}
	
	private int dropConsumerGroup(long domainId,String topicName, String groupName) throws Exception {
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(groupName==null || groupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		if(ConsumerGroup.DEFAULT_GROUP_NAME.equals(groupName.toLowerCase())) {
			throw new Exception("consumer group name is default");
		}
		
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		ConsumerGroup group = new ConsumerGroup();
		group.setName(groupName);
		topic.setConsumerGroup(group);
		
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_DROP_CONSUMER_GROUP,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		return 0;
	}


	@Override
	public List<ConsumerGroup> getConsumerGroupList(String topicName) throws Exception {
		updateSession();
		return getConsumerGroupList(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private List<ConsumerGroup> getConsumerGroupList(long domainId,String topicName) throws Exception {
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GET_CONSUMER_GROUP_LIST,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			byte[] data = response.getData();
			if(data==null || data.length==0) {
				return null;
			}
			String jsonString = new String(data);
			return JSON.parseArray(jsonString, ConsumerGroup.class);
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

	public int grantUserToConsumerGroup(String topicName, String groupName, String userName) throws Exception {
		updateSession();
		return grantUserToConsumerGroup(MQDomain.DEFAULT_DOMAIN_ID, topicName, groupName, userName);
	}
	
	private int grantUserToConsumerGroup(long domainId,String topicName,String groupName, String userName)
			throws Exception {
		if(userName==null || userName.trim().length()==0){
			throw new Exception("user is null");
		}
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(groupName==null || groupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		User user = new User(userName.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		topic.setUser(user);
		
		ConsumerGroup group = new ConsumerGroup();
		group.setName(groupName);
		topic.setConsumerGroup(group);
		
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GRANT_USER_TO_CONSUMER_GROUP,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		
		return 0;
	}
	

	@Override
	public int removeConsumerUser(String topicName, String groupName, String userName) throws Exception {
		updateSession();
		return removeUserFromConsumerGroup(MQDomain.DEFAULT_DOMAIN_ID,topicName,groupName,userName);
	}
	
	private int removeUserFromConsumerGroup(long domainId,String topicName, String groupName, String userName) throws Exception {
		if(userName==null || userName.trim().length()==0){
			throw new Exception("user is null");
		}
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		if(groupName==null || groupName.trim().length()==0) {
			throw new Exception("consumer group name is null");
		}
		
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		User user = new User(userName.toLowerCase());
		user.setDomain(new MQDomain(domainId));
		topic.setUser(user);
		
		ConsumerGroup group = new ConsumerGroup();
		group.setName(groupName);
		topic.setConsumerGroup(group);
		
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_REMOVE_CONSUMER_USER,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			return response.getIntAttribute(TopicMapping.RESULT);
		}
		
		if(ResponseStatus.STATUS_TIMEOUT.equals(response.getStatus())){
			throw new TimeoutException(response.getException());
		}
		
		String exceptionStr = response.getException();
		if(exceptionStr!=null && exceptionStr.trim().length()>0){
			throw new Exception(exceptionStr);
		}
		
		return 0;
	}


	@Override
	public List<User> getConsumerUserList(String topicName) throws Exception {
		updateSession();
		return getConsumerUserList(MQDomain.DEFAULT_DOMAIN_ID, topicName);
	}
	
	private List<User> getConsumerUserList(long domainId,String topicName) throws Exception {
		if(topicName==null || topicName.trim().length()==0){
			throw new Exception("topic name is null");
		}
		MQTopic topic = new MQTopic();
		topic.setName(topicName);
		topic.setDomain(new MQDomain(domainId));
		topic.buildTopicId();
		
		ClientCommand command = new TopicCommand(TopicCommand.ACTION_GET_CONSUMER_USER_LIST,topic);
		command.setSessionId(session.getId());
		MQResponse response = writeMasterMessage(command);
		if(response==null){
			throw new ConnectionException(getConnectionExceptionMsg());
		}
		
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			byte[] data = response.getData();
			if(data==null || data.length==0) {
				return null;
			}
			String jsonString = new String(data);
			return JSON.parseArray(jsonString, User.class);
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


	public void updateSession()throws Exception{
		MessageClient masterClient =clientService.getMasterClient();
		updateSession(masterClient);
	}
	
	@Override
	public String getSessionId() {
		if(session==null){
			return null;
		}
		return session.getId();
	}
	
	public MessageClient getMasterClient() throws Exception {
		return clientService.getMasterClient();
	}

	private MQResponse writeMasterMessage(ClientCommand command)throws Exception{
		MessageClient masterClient =clientService.getMasterClient();
		return masterClient.writeMessage(command);
	}

	public void logout()throws Exception{
		MessageClient masterClient =clientService.getMasterClient();
		logout(masterClient);
	}

	

	private long getDomainId(String name)throws Exception{
		throw new Exception("implement in future");
	}
	
	private String getConnectionExceptionMsg() throws Exception {
		return "not connect to master server";
	}
	
	
}
