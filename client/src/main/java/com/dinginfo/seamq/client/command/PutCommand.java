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
package com.dinginfo.seamq.client.command;

import java.util.List;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.common.ProtoUtil;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;
import com.google.protobuf.ByteString;



/** 
 * @author David Ding
 *
 */

public class PutCommand extends AbstractCommand {
	private MQMessageExt message;
	
	public PutCommand(MQMessageExt message){
		this.message= message;
	}

	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(Command.COMMAND_PUT);
		requestBuilder.setRequestId(this.uuid);
		if(sessionId!=null){
			requestBuilder.setSessionId(sessionId);
		}
		
		if(message.getData()!=null) {
			requestBuilder.setData(ByteString.copyFrom(message.getData()));
		}
		ProtoUtil.setField(requestBuilder, MessageMapping.FIELD_QUEUE_ID, message.getQueue().getId());
		ProtoUtil.setField(requestBuilder, MessageMapping.FIELD_QUEUE, message.getQueue().getQueue());
		
		if(message.getArraySize()>0){
			ProtoUtil.setField(requestBuilder, MessageMapping.FIELD_ARRAY_SIZE, message.getArraySize());
		}
		List<DataField> attributeList = message.getAttributeList();
		if(attributeList!=null){
			DataFieldPro.Builder attBuilder =null;
			for(DataField attribute : attributeList){
					attBuilder = DataFieldPro.newBuilder();
					attBuilder.setKey(attribute.getKey());
					attBuilder.setDataType(attribute.getDataType());
					attBuilder.setData(ByteString.copyFrom(attribute.getData()));
					requestBuilder.addAttributes(attBuilder);
			}
		}
		return requestBuilder.build();
	}

	

}
