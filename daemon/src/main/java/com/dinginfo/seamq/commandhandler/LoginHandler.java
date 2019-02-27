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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.common.UUIDHexGenerator;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.SessionMapping;
import com.dinginfo.seamq.entity.mapping.UserMapping;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.service.UserService;

public class LoginHandler implements CommandHandler,ResponseStatus {
	private final static Logger logger = LogManager.getLogger(LoginHandler.class);
	
	private SessionService sessionService = null;

	private UserService userService =null;
	
	public LoginHandler(){
		this.sessionService = MyBean.getBean(SessionService.BEAN_NAME, SessionService.class);
		this.userService =  MyBean.getBean(UserService.BEAN_NAME,
				UserService.class);
		
	}

	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {
		try {
			User u = new User();
			u.setName(request.getStringAttribute(UserMapping.FIELD_NAME));
			u.setPwd(request.getStringAttribute(UserMapping.FIELD_PASSWD));
			u.setType(request.getStringAttribute(UserMapping.FIELD_TYPE));
			u.setDomain(new MQDomain(request.getLongAttribute(DomainMapping.FIELD_DOMAIN_ID)));
			
			User user = userService.getUserByPK(u);
			if (user != null) {
				String pwd = userService.getPassword(u);
				if (pwd != null && pwd.equals(u.getPwd())) {

					String sid = UUIDHexGenerator.generate();
					sid = StringUtil.generateMD5String(sid);

					MQSession session = new MQSession();
					session.setId(sid);
					session.setUser(user);
					Date date = Calendar.getInstance().getTime();
					session.setUpdatedTime(date);
					sessionService.create(session);
					
					response.setAttribute(SessionMapping.FIELD_SID, sid);
					response.setAttribute(DomainMapping.FIELD_DOMAIN_ID, user.getDomain().getId());
						
					
				} else {
					response.setStatus(STATUS_ERROR);
					response.setException("user or password is invalid");
				}
			} else {
				response.setStatus(STATUS_ERROR);
				response.setException("user or password is invalid");
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
