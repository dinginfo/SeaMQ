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

package com.dinginfo.seamq.commandhandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.entity.mapping.UserMapping;
import com.dinginfo.seamq.service.UserService;

public class UserHandler implements CommandHandler,ResponseStatus{
	private final static Logger logger =LogManager.getLogger(UserHandler.class);
	
	public static final int ACTION_GET=1;
	public static final int ACTION_CREATE=2;
	public static final int ACTION_UPDATE=3;
	public static final int ACTION_UPDATE_PASSWD=4;
	public static final int ACTION_DELETE=5;
	public static final int ACTION_COUNT=6;
	public static final int ACTION_LIST=7;

	
	protected UserService userService = null;

	private int action =0;
	
	public UserHandler(int actionType){
		this.userService = MyBean.getBean(UserService.BEAN_NAME,
				UserService.class);
		this.action = actionType;
	}
	
	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {

		switch(action){
		case ACTION_GET:
			get(request,response);
			break;
		case ACTION_CREATE:
			create(request,response);
			break;
		case ACTION_UPDATE:
			update(request, response);
			break;
		case ACTION_DELETE:
			delete(request,response);
			break;
		case ACTION_UPDATE_PASSWD:
			updatePasswd(request,response);
			break;
		case ACTION_COUNT:
			count(request, response);
			break;
		case ACTION_LIST:
			list(request, response);
			break;
		}
		
	}



	protected User mappingUser(MessageRequest request) {
		User user = new User();
		user.setName(request.getStringAttribute(UserMapping.FIELD_NAME));
		user.setPwd(request.getStringAttribute(UserMapping.FIELD_PASSWD));
		user.setStatus(request.getStringAttribute(UserMapping.FIELD_STATUS));
		user.setType(request.getStringAttribute(UserMapping.FIELD_TYPE));
		user.setRemark(request.getStringAttribute(UserMapping.FIELD_REMARK));
		user.setDomain(new MQDomain(request.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID)));
		return user;
	}
	
	private void get(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}

		try {
			User u = userService.getUserByPK(user);
			if(u!=null){
				response.setAttribute(DomainMapping.FIELD_DOMAIN_ID,u.getDomain().getId());
				response.setAttribute(UserMapping.FIELD_NAME, u.getName());
				response.setAttribute(UserMapping.FIELD_STATUS, u.getStatus());
				response.setAttribute(UserMapping.FIELD_REMARK, u.getRemark());
				Date date = u.getCreatedTime();
				if(date!=null){
					response.setAttribute(UserMapping.FIELD_CREATED_TIME, date.getTime());
				}
				date = u.getUpdatedTime();
				if(date!=null){
					response.setAttribute(UserMapping.FIELD_UPDATED_TIME, date.getTime());
				}
			}
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
			
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void create(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}

		try {
			Date date =Calendar.getInstance().getTime();
			user.setCreatedTime(date);
			user.setUpdatedTime(date);
			String pwd = StringUtil.generateSHA512String(user.getPwd());
			user.setPwd(pwd);
			userService.createUser(user);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
			
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void update(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}

		try {
			Date date =Calendar.getInstance().getTime();
			user.setUpdatedTime(date);
			int n =userService.updateUser(user);
			response.setStatus(STATUS_SUCCESS);
			response.setAttribute(UserMapping.RESULT, n);
			response.writeMessage();
			
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void delete(MessageRequest request, MessageResponse response) {	
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}
		try {
			int n = userService.deleteUserByPK(user);
			response.setAttribute(UserMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
			
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void updatePasswd(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}
		
		try {
			Date date = Calendar.getInstance().getTime();
			user.setUpdatedTime(date);
			String pwd = StringUtil.generateSHA512String(user.getPwd());
			user.setPwd(pwd);
			int n = userService.updateUserPassword(user);
			response.setAttribute(UserMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void count(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}
		
		try {
			long n = userService.getUserCount(user.getDomain().getId());
			response.setAttribute(UserMapping.RESULT, n);
			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void list(MessageRequest request, MessageResponse response) {
		User user = mappingUser(request);
		if(user==null){
			response.writeMessage(STATUS_ERROR, "not user info");
			return;
		}
		
		try {
			int pageSize =request.getIntAttribute(TopicMapping.FIELD_PAGE_SIZE);
			int pageNo =request.getIntAttribute(TopicMapping.FIELD_PAGE_NO);
			List<User> userList = null;
			userList = userService.getUserList(user.getDomain().getId(),pageNo,pageSize);
			if(userList!=null && userList.size()>0){
				String jsonString = JSON.toJSONString(userList);
				response.setData(jsonString.getBytes());			
			}

			response.setStatus(STATUS_SUCCESS);
			response.writeMessage();
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}

}
