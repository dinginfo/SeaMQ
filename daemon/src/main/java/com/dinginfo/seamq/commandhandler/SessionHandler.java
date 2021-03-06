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
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.service.SessionService;

public class SessionHandler implements CommandHandler, ResponseStatus {
	private static final Logger logger = LogManager.getLogger(SessionHandler.class);
	
	private SessionService sessionService = null;
	
	public SessionHandler(){
		this.sessionService =  MyBean.getBean(
				SessionService.BEAN_NAME, SessionService.class);
	}

	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {

		try {
			String sid = request.getSessionId();
			MQSession session = new MQSession();
			session.setId(sid);
			Date date = Calendar.getInstance().getTime();
			session.setUpdatedTime(date);
			int n =sessionService.update(session);
			if(n>0){
				response.setStatus(STATUS_SUCCESS);
			}else{
				response.setStatus(STATUS_ERROR);
			}
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
