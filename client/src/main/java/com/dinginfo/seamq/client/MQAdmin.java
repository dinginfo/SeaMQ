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

import java.util.List;

import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;

public interface MQAdmin {

	public void createUser(User user) throws Exception;
	
	public int updateUser(User user) throws Exception;

	public int updateUserPassword(User user) throws Exception;

	public int deleteUser(String userId) throws Exception;

	public User getUserByPK(String userId) throws Exception;
	
	public long getUserCount()throws Exception;
	
	public List<User> getUserList(int pageSize,int pageNo)throws Exception;
	
	public List<User> getUserListByTopic(String topicName)throws Exception;

	public void createTopic(MQTopic topic) throws Exception;
	
	public int updateTopic(MQTopic topic) throws Exception;

	public int dropTopic(String topicName) throws Exception;

	public MQTopic getTopic(String topicName) throws Exception;
	
	public long getTopicCount()throws Exception;

	public List<MQTopic> getTopicList(int pageSize, int pageNo) throws Exception;

	public int disableTopic(String topicName) throws Exception;

	public int enableTopic(String topicName) throws Exception;

	public int disableQueue(String topicName) throws Exception;

	public int enableQueue(String topicName) throws Exception;

	public int increaseQueue(String topicName) throws Exception;

	public int increaseQueue(String topicName, int qty) throws Exception;

	public int reduceQueue(String topicName) throws Exception;

	public MQueue getQueueInfo(String topicName, int queueIndex) throws Exception;

	public List<MQueue> getQueueList(String topicName, int pageSize, int pageNo) throws Exception;
	
	public void createConsumerGroup(String topicName,String groupName)throws Exception;
	
	public int dropConsumerGroup(String topicName,String groupName)throws Exception;
	
	public List<ConsumerGroup> getConsumerGroupList(String topicName)throws Exception;

	public int grantUserToProducer(String topicName, String userName) throws Exception;
	
	public int removeProducerUser(String topicName, String userName) throws Exception;
	
	public List<User> getProducerUserList(String topicName)throws Exception;

	public int grantUserToConsumerGroup(String topicName, String groupName, String userName) throws Exception;
	
	public int removeConsumerUser(String topicName, String groupName, String userName) throws Exception;
	
	public List<User> getConsumerUserList(String topicName)throws Exception;

	public void logout() throws Exception;

	public void updateSession() throws Exception;
	
	public String getSessionId();


}
