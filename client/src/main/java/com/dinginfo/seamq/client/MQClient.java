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

import java.util.Date;

import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.client.command.LoginCommand;
import com.dinginfo.seamq.client.command.LogoutCommand;
import com.dinginfo.seamq.client.command.UpdateSessionCommand;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.SessionMapping;

public abstract class MQClient {
	public static final long SESSION_TIME_INTERVAL=30 * 60 * 1000; 
	
	public static final long SLEEP_TIME=1000;
	
	protected MQClientService clientService;
	
	protected  MQSession session;
	
	protected long domainId = 0;
	
	protected MessageClient getMessageClientByQueueId(String queueId)throws Exception{
		return clientService.getMessageClientByQueueId(queueId);
	}
	
	protected String getQueueLocation(String queueId)throws Exception{
		return clientService.getQueueLocation(queueId);
		
	}
	
	protected String login(long currentDomainId,String userId,String password,MessageClient client)throws Exception{
		if(userId==null || userId.trim().length()==0){
			throw new Exception("user is null");
		}
		if(password==null || password.trim().length()==0){
			throw new Exception("password is null");
		}
		String pwd = StringUtil.generateSHA512String(password);
		User user = new User();
		user.setName(userId);
		user.setPwd(pwd);
		user.setDomain(new MQDomain(currentDomainId));
		if(currentDomainId==0){
			user.setType(User.TYPE_ADMIN);
		}
		ClientCommand command = new LoginCommand(user);
		MQResponse response =client.writeMessage(command);
		if(response==null){
			return null;
		}
		if(response.getException()!=null && response.getException().trim().length()>0){
			throw new Exception(response.getException());
		}
		
		String sessionId =response.getStringAttribute(SessionMapping.FIELD_SID);
		if(sessionId!=null){
			session = new MQSession();
			session.setId(sessionId);
			Date date = new Date(System.currentTimeMillis());
			session.setCreatedTime(date);
			session.setUpdatedTime(date);
		}
		domainId = response.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID);
		return sessionId;
	}
	
	protected void logout(MessageClient client)throws Exception{
		if(session==null){
			return ;
		}
		if(client==null){
			return;
		}
		ClientCommand command = new LogoutCommand(session.getId());
		MQResponse response =client.writeMessage(command);
		if(response==null){
			return ;
		}
		if(response.getException()!=null && response.getException().trim().length()>0){
			throw new Exception(response.getException());
		}
	}
	
	public void logout()throws Exception{
		logout(clientService.getBroker());
	}
	
	protected void updateSession(MessageClient client)throws Exception{
		if(session==null){
			return ;
		}
		if(client==null){
			return;
		}
		Date updatedTime = session.getUpdatedTime();
		long time = System.currentTimeMillis() - updatedTime.getTime();
		if(time<SESSION_TIME_INTERVAL){
			return;
		}
		ClientCommand command = new UpdateSessionCommand(session.getId());
		MQResponse response = client.writeMessage(command);
		if(response==null){
			return ;
		}
		if(ResponseStatus.STATUS_SUCCESS.equals(response.getStatus())){
			Date date = new Date(System.currentTimeMillis());
			session.setUpdatedTime(date);
			return;
		}
		if(response.getException()!=null && response.getException().trim().length()>0){
			throw new Exception(response.getException());
		}
	}
	
	
	
	public void shutdown(){
		if(clientService!=null){
			clientService.shutdown();
		}
	}
}
