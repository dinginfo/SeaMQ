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
package com.dinginfo.seamq.broker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.MessageServerHandler;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ServiceContext;
import com.dinginfo.seamq.commandhandler.CommandHandler;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.scheduler.CommondHandlerTask;

import io.netty.channel.ChannelHandlerContext;

public class BrokerMessageServerHandler extends MessageServerHandler {
	private final static Logger logger = LogManager.getLogger(BrokerMessageServerHandler.class);
	
	private ExecutorService executorService;
	
	private LinkedBlockingQueue taskQueue;
	
	private int maxTaskQueueSize = 1000 * 1000;
	
	public BrokerMessageServerHandler(ServiceContext context,ExecutorService executorService,LinkedBlockingQueue taskQueue){
		super(context);
		this.executorService = executorService;
		this.taskQueue = taskQueue;
		this.maxTaskQueueSize = config.getMaxTaskQueueSize();
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
	
	public BrokerMessageServerHandler(ServiceContext context){
		super(context);
	}
	
	public void doHandle0(MessageRequest request, MessageResponse response){
		CommandHandler handler = BrokerCommandHandlerFactory.geCommandHandler(request
				.getServiceName());
		if(BrokerCommandHandlerFactory.isDriectAccess(request.getServiceName())){
			handler.doCommand(request, response);	
		}else{
			try {
				long timeout =request.getSessionTimeout();
				MQSession session = sessionService.get(request.getSessionId(),timeout);
				if (session ==null) {
					response.writeMessage(ResponseStatus.STATUS_TIMEOUT, "session timeout");
					return;
				}
				request.setUser(session.getUser());
				handler.doCommand(request, response);
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	@Override
	protected void doHandle(MessageRequest request, MessageResponse response) {
		if(taskQueue.size()<maxTaskQueueSize){
			CommondHandlerTask task = new CommondHandlerTask(request, response, this);
			executorService.submit(task);
		}else{
			doHandle(request, response);
		}
	}

}
