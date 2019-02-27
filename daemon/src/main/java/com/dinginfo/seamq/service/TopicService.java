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

import java.util.List;

import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;
import com.dinginfo.seamq.entity.User;

public interface TopicService {
	public static final String BEAN_NAME="topicService";
	
	public void createTopic(MQTopic topic) throws Exception;
	
	public int updateTopic(MQTopic topic) throws Exception;

	public int disableTopic(MQTopic topic) throws Exception;
	
	public int enableTopic(MQTopic topic) throws Exception;

	public int dropTopic(MQTopic topic) throws Exception;
	
	public int disableQueue(MQueue queue)throws Exception;
	
	public int enableQueue(MQueue queue)throws Exception;
	
	public int increaseQueue(MQTopic topic, int queueNum)throws Exception;
	
	public int reduceQueue(MQTopic topic, String queueId)throws Exception;

	public MQTopic getTopicByPK(String key) throws Exception;
	
	public void createConsumerGroup(ConsumerGroup group)throws Exception;
	
	public int dropConsumerGroupByPK(ConsumerGroup group)throws Exception;
	
	public ConsumerGroup getConsumerGroupByPK(ConsumerGroup group)throws Exception;
	
	public List<ConsumerGroup> getConsumerGroupList(String topicId)throws Exception;
	
	public int grantUserToProducer(MQTopic topic)throws Exception;
	
	public int removeProducerUser(MQTopic topic)throws Exception;
	
	public User getProducerUserByPK(MQTopic topic)throws Exception;
	
	public List<User> getProducerUserList(String topicId)throws Exception;
	
	public int grantUserToConsumerGroup(ConsumerGroup group)throws Exception;
	
	public int removeConsumerUser(ConsumerGroup group)throws Exception;
	
	public User getConsumerUserByPK(ConsumerGroup group)throws Exception;
	
	public List<User> getConsumerUserList(String topicId)throws Exception;
	
	public MQueue getQueueByPK(MQueue queue)throws Exception;
	
	public List<MQueue> getQueueList(MQTopic topic)throws Exception;
	
	public int updateQueueInOffset(MQueue queue)throws Exception;
	
	public int updateQueueOutOffset(MQueue queue)throws Exception;
	
	public int getTopicCount()throws Exception;
	
	public List<MQTopic> getTopicList(MQTopic topic,int pageSize,int pageNo)throws Exception;
	
	public List<User> getUserListByTopic(String topicId)throws Exception;
	
	public List<OutPosition> getOutPositionList(String queueId)throws Exception;
	
	public boolean load()throws Exception;
	
	public void clear();
	
	public boolean isLoaded();
}
