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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;
import com.dinginfo.seamq.service.SessionService;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author David Ding
 * 
 */
public abstract class MessageServerHandler extends SimpleChannelInboundHandler<Object> {
	protected SessionService sessionService = MyBean.getBean(
			SessionService.BEAN_NAME, SessionService.class);

	protected ServiceContext serverContext = null;
	protected long sessionTimeout = 0;
	protected ServerConfig config = null;

	public MessageServerHandler(ServiceContext context) {
		this.serverContext = context;
		this.config = context.getConfig();
		this.sessionTimeout = config.getSessionTimeout();
	}

	

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		RequestPro req = (RequestPro) msg;
		if(req==null){
			return;
		}

		Channel channel = ctx.channel();
		MessageRequest request = new MessageRequest(channel);
		request.setServiceName(req.getServiceName());
		request.setSessionTimeout(sessionTimeout);
		request.setRequestId(req.getRequestId());
		request.setSessionId(req.getSessionId());
		request.setServiceName(req.getServiceName());
		if(req.getData()!=null) {
			request.setData(req.getData().toByteArray());
		}		
		request.setServerContext(serverContext);
		

		List<DataFieldPro> fieldList = req.getFieldsList();
		if(fieldList!=null){
			Map<String,DataField> fieldMap = new HashMap<String, DataField>();
			DataField field = null;
			for(DataFieldPro fd : fieldList){
				field = new DataField(fd.getKey(), fd.getDataType(), fd.getData().toByteArray());
				fieldMap.put(field.getKey(),field);
			}
			request.setFieldMap(fieldMap);
		}
		
		List<DataFieldPro> attriList = req.getAttributesList();
		if (attriList != null) {
			Map<String, DataField> attributeMap = new HashMap<String, DataField>();
			DataField attribute = null;
			for (DataFieldPro attri : attriList) {
				attribute = new DataField(attri.getKey(),attri.getDataType(),attri.getData().toByteArray());
				attributeMap.put(attri.getKey(), attribute);
			}
			request.setAttributeMap(attributeMap);
		}

		
		
		MessageResponse response = new MessageResponse(channel);
		response.setRequestId(req.getRequestId());
		doHandle(request, response);
	}
	
	protected abstract void doHandle(MessageRequest request, MessageResponse response);
}
