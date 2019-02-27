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
package com.dinginfo.seamq.master;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.MessageServerHandler;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ServiceContext;
import com.dinginfo.seamq.commandhandler.CommandHandler;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.entity.User;

import io.netty.channel.ChannelHandlerContext;

public class MasterMessageServerHandler extends MessageServerHandler {
	private final static Logger logger = LogManager.getLogger(MasterMessageServerHandler.class);

	public MasterMessageServerHandler(ServiceContext context){
		super(context);
	}
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("connected");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		logger.info(cause.getMessage());
	}
	

	@Override
	protected void doHandle(MessageRequest request, MessageResponse response) {
		String serviceName = request.getServiceName();
		CommandHandler handler = MasterCommandHandlerFactory
				.geCommandHandler(serviceName);
		if(handler==null){
			StringBuilder sb =new StringBuilder(50);
			sb.append("no service:");
			sb.append(serviceName);
			response.writeMessage(ResponseStatus.STATUS_ERROR, sb.toString());
			return;
		}
		if (Command.COMMAND_LOGIN.equals(serviceName)
				|| Command.COMMAND_LOGOUT.equals(serviceName)) {
			handler.doCommand(request, response);
		} else {
			try {
				long timeout =request.getSessionTimeout();
				MQSession session = sessionService.get(request.getSessionId(),timeout);
				if (session ==null) {
					response.writeMessage(ResponseStatus.STATUS_TIMEOUT, "session timeout");
					return;
				}
				
				if(Command.COMMAND_QUEUE_SET_LOCATION.equals(serviceName)){
					handler.doCommand(request, response);
					return;
				}
				
				if (!User.TYPE_ADMIN.equals(session.getUser().getType())) {
					response.writeMessage(ResponseStatus.STATUS_ERROR, "not admin permission");
					return;
				}
				handler.doCommand(request, response);
			} catch (Exception e) {
				logger.error(e.getMessage());
				response.writeMessage(ResponseStatus.STATUS_ERROR, e.getMessage());
				
			}
		}

	}

}
