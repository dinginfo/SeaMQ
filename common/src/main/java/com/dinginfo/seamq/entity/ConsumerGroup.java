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
package com.dinginfo.seamq.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.MQConstant;
import com.dinginfo.seamq.common.StringUtil;

public class ConsumerGroup implements Serializable {
	public static final String DEFAULT_GROUP_NAME = "default";
	
	private String id;
	
	private String name;
	
	private MQTopic topic;
	
	private User user;
	
	private Map<String,User> userMap;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MQTopic getTopic() {
		return topic;
	}

	public void setTopic(MQTopic topic) {
		this.topic = topic;
	}
	
	public void setUser(User user) {
		this.user = user;
	}

	public User getUser() {
		return user;
	}

	public Map<String, User> getUserMap() {
		if(userMap!=null) {
			return userMap;
		}
		if(user!=null) {
			Map<String,User> map = new HashMap<String,User>();
			map.put(user.getName(), user);
			return map;
		}else {
			return null;
		}
	}

	public void putUser(User user) {
		if(this.user==null && userMap==null) {
			this.user = user;
			return;
		}
		if(userMap==null) {
			createUserMap();
			userMap.put(this.user.getName(), this.user);
			this.user = null;
		}
		userMap.put(user.getName(), user);
	}
	
	private synchronized void createUserMap() {
		if(userMap!=null) {
			return ;
		}
		userMap = new HashMap<String,User>();
	}
	
	public User getUser(String username) {
		if(userMap!=null) {
			return userMap.get(username);
		}
		if(user==null || user.getName()==null) {
			return null;
		}
		if(user.getName().equals(username)) {
			return user;
		}
		return null;
	}
	
	public User deleteUser(String username) {
		if(userMap!=null) {
			User u = userMap.remove(username);
			if(userMap.size()==0) {
				userMap = null;
			}
			return u;
		}
		if(user!=null && user.getName().equals(username)) {
			User u = user;
			user = null;
			return u;
		}
		return null;
	}
	
	public List<User> getUserList(){
		List<User> userList = new ArrayList<User>();
		if(userMap!=null && userMap.size()>0) {
			for(User u : userMap.values()) {
				userList.add(u);
			}
		}else if(user!=null) {
			userList.add(user);
		}
		return userList;
	}
	
	public String buildObjectID(String topicId,String groupName) throws Exception {
		if(topic==null || topic.getId()==null) {
			throw new Exception("topic is null");
		}
		if(name==null || name.trim().length()==0) {
			throw new Exception("group name is null");
		}
		StringBuilder sb = new StringBuilder(64);
		sb.append(topicId);
		sb.append(MQConstant.SEPARATOR);
		sb.append(groupName);
		return StringUtil.generateMD5String(sb.toString());
	}
	
	public String buildObjectID() throws Exception {
		if(topic==null || topic.getId()==null) {
			throw new Exception("topic is null");
		}
		if(name==null || name.trim().length()==0) {
			throw new Exception("group name is null");
		}
		this.id = buildObjectID(topic.getId(), name); 
		return id;
	}

}
