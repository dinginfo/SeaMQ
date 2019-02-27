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

import io.netty.channel.Channel;

import com.dinginfo.seamq.entity.User;

public class MessageRequest extends MQRequest {
	private Channel channel;
	
	private long sessionTimeout;
	
	private long sessionSaveInterval;
	
	private User user;
	
	private ServiceContext serverContext;
	
	public MessageRequest(Channel channel){
		this.channel = channel;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public long getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(long sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	public long getSessionSaveInterval() {
		return sessionSaveInterval;
	}

	public void setSessionSaveInterval(long sessionSaveInterval) {
		this.sessionSaveInterval = sessionSaveInterval;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public ServiceContext getServerContext() {
		return serverContext;
	}

	public void setServerContext(ServiceContext serverContext) {
		this.serverContext = serverContext;
	}
	
	
}
