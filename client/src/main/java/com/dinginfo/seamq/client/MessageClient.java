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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.common.UUIDHexGenerator;


/** 
 * @author David Ding
 *
 */
public class MessageClient {
	

	private final static Logger logger= LogManager.getLogger(MessageClient.class);
	private Map<String, Command> commandMap ;
	private long OPERATE_TIME_OUT = 10000L;
	private MessageConnector connector;

	private InetSocketAddress address;
	
	
	public MessageClient(MessageClientContext context,InetSocketAddress address){
		this.address = address;
		commandMap = context.getCommandMap();
		connector = new NioMessageConnector(context, address);
		if(context.getOperateTimeout()>0){
			this.OPERATE_TIME_OUT = context.getOperateTimeout();
		}
	}

	public boolean isContected(){
		if(connector!=null){
			return connector.checkConnected();
		}else{
			return false;	
		}
		 
	}


	public boolean contect(){
		if(connector!=null){
			return connector.connect();
		}else{
			return false;
		}
	}
	
	public void shutdown() {
		connector.shutdown();
	}

	public MQResponse writeMessage(ClientCommand clientCommand) {
		if (connector == null) {
			return null;
		}
	
		MQResponse result = null;
		String key=null;
		String uuid=null;
		
		String status=null;
		ClientCommand command=null;

		try {
			command=clientCommand;
			uuid=UUIDHexGenerator.generate();
			command.setUuid(uuid);
			commandMap.put(uuid, command);	
			command.resetCountDownLatch();
			if(connector.checkConnected()){
				connector.writeMessage(command.getSendObject());	
			}else{
				return null;
			}
			try {
				//latchWait(command, OPERATE_TIME_OUT);
				if(!command.getLatch().await(OPERATE_TIME_OUT, TimeUnit.MILLISECONDS)){
					StringBuilder sb=new StringBuilder();
					sb.append("Timed out(");
					sb.append(OPERATE_TIME_OUT);
					sb.append(") waiting for operation");
					logger.info(sb.toString());
				}
			} catch (InterruptedException ex) {
				logger.info(ex.getMessage());
			}
			command.decode();
			status=command.getStatus();
			result =command.getResult();
			
		} finally {
			commandMap.remove(uuid);
		}
		return result;
	}

	private void latchWait(final ClientCommand cmd, final long timeout)
			throws InterruptedException, TimeoutException {
		if (!cmd.getLatch().await(timeout, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException("Timed out(" + timeout
					+ ") waiting for operation");
		}
	}

	
	
}
