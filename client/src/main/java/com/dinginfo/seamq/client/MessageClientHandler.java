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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.MQResponse;
import com.dinginfo.seamq.client.command.ClientCommand;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.entity.mapping.SessionMapping;
import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.ResponseProto.ResponsePro;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


/** 
 * @author David Ding
 *
 */
public class MessageClientHandler extends SimpleChannelInboundHandler<Object> {
	private Map<String,Command> commandMap;
	
	private MessageClientContext clientContext;
	public MessageClientHandler(MessageClientContext context) {
		this.commandMap=context.getCommandMap();
		this.clientContext = context; 
	}


	@Override
	protected void channelRead0(ChannelHandlerContext context, Object obj)
			throws Exception {
		ResponsePro  resp=(ResponsePro)obj;
		String requestId=resp.getRequestId();
		MQResponse response = new MQResponse();
		response.setVersion(resp.getVersion());
		response.setStatus(resp.getStatus());
		response.setException(resp.getException());
		if(resp.getData()!=null) {
			response.setData(resp.getData().toByteArray());
		}
		//attribute list
		List<DataFieldPro> attriList  = resp.getAttributesList();
		if(attriList!=null && attriList.size()>0){
			Map<String,DataField> attributeMap = new HashMap<String, DataField>();
			DataField attribute =null;
			for(DataFieldPro attri : attriList){
				attribute =new DataField(attri.getKey(), attri.getDataType(), attri.getData().toByteArray());
				attributeMap.put(attribute.getKey(), attribute);
			}
			response.setAttributeMap(attributeMap);
		}
		
		//field list
		List<DataFieldPro> fieldList = resp.getFieldsList();
		if(fieldList!=null && fieldList.size()>0){
			Map<String,DataField> fieldMap = new HashMap<String, DataField>();
			DataField attribute =null;
			for(DataFieldPro field : fieldList){
				attribute =new DataField(field.getKey(), field.getDataType(), field.getData().toByteArray());
				fieldMap.put(attribute.getKey(), attribute);
			}
			response.setFieldMap(fieldMap);
		}
	
		String serviceName = resp.getServiceName();
		if(Command.COMMAND_NOTIFY.equals(serviceName)){
			MessageNotifyHandler notifyHandler = clientContext.getMessageNotifyHandler();
			if(notifyHandler==null){
				return;
			}
			String topicName = response.getStringField(MessageMapping.FIELD_TOPIC_NAME);
			int queueName = response.getIntField(MessageMapping.FIELD_QUEUE);
			String sessionId = response.getStringField(SessionMapping.FIELD_SID);
			MessageNotifyContext notifyContext = new MessageNotifyContext();
			notifyContext.setTopicName(topicName);
			notifyContext.setQueue(queueName);
			notifyContext.setSessionId(sessionId);
			notifyHandler.doHandle(notifyContext);
			return;
		}
		
		Command cmd=commandMap.get(requestId);
		if(cmd!=null){
			((ClientCommand)cmd).setResult(response);
			((ClientCommand)cmd).countDown();
		}
		
	}



}
