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

import java.util.HashMap;

import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.ResponseProto.ResponsePro;
import com.google.protobuf.ByteString;

public class MessageResponse extends MQResponse {
	private Channel channel;
	
	private String requestId;
	
	public MessageResponse(Channel channel){
		this.channel = channel;
		this.attributeMap = new HashMap<String, DataField>();
	}
	
	public void writeMessage(){
		ResponsePro.Builder resBuilder = ResponsePro.newBuilder();
		resBuilder.setServiceName(Command.COMMAND_RESPONSE);
		if(requestId!=null){
			resBuilder.setRequestId(requestId);	
		}
		
		if(status!=null){
			resBuilder.setStatus(status);
		}
		
		if(exception!=null){
			resBuilder.setException(exception);
		}
		if(data!=null && data.length>0) {
			resBuilder.setData(ByteString.copyFrom(data));
		}
		
		if(fieldMap!=null && fieldMap.size()>0){
			DataFieldPro.Builder fieldBuilder = null;
			for(DataField field : fieldMap.values()){
				fieldBuilder = DataFieldPro.newBuilder();
				fieldBuilder.setKey(field.getKey());
				fieldBuilder.setDataType(field.getDataType());
				fieldBuilder.setData(ByteString.copyFrom(field.getData()));
				resBuilder.addFields(fieldBuilder.build());
			}
		}
		
		if(attributeMap!=null && attributeMap.size()>0){
			DataFieldPro.Builder attBuilder = null;
			for(DataField attr : attributeMap.values()){
				attBuilder = DataFieldPro.newBuilder();
				attBuilder.setKey(attr.getKey());
				attBuilder.setDataType(attr.getDataType());
				attBuilder.setData(ByteString.copyFrom(attr.getData()));
				resBuilder.addAttributes(attBuilder.build());
			}
		}
		channel.writeAndFlush(resBuilder);
	}
	
	public void writeMessage(String status,String msg){
		ResponsePro.Builder resBuilder = ResponsePro.newBuilder();
		resBuilder.setServiceName(Command.COMMAND_RESPONSE);
		resBuilder.setRequestId(requestId);
		resBuilder.setStatus(status);
		resBuilder.setException(msg);
		channel.writeAndFlush(resBuilder);
	}



	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	
}
