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

import com.dinginfo.seamq.common.ProtoUtil;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.mapping.QueueMapping;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class QueueCommand extends AbstractCommand{
	public static final int ACTION_GET=1;
	public static final int ACTION_SET=2;
	public static final int ACTION_GET_LOCATION=3;
	public static final int ACTION_SET_LOCATION=4;
	public static final int ACTION_GET_LIST=5;
	
	private MQueue queue ; 
	
	protected String serviceName; 
	
	private int pageSize;
	
	private int pageNo;
	
	public QueueCommand(int actionType,MQueue queue){
		this.queue =queue;
		switch(actionType){
		case ACTION_GET:
			serviceName = COMMAND_QUEUE_GET;
			break;
		case ACTION_GET_LOCATION:
			serviceName = COMMAND_QUEUE_GET_LOCATION;
			break;
		case ACTION_SET_LOCATION:
			serviceName = COMMAND_QUEUE_SET_LOCATION;
			break;
		case ACTION_GET_LIST:
			serviceName = COMMAND_QUEUE_LIST;
			break;
		}
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(this.serviceName);
		requestBuilder.setRequestId(this.uuid);
		if(sessionId!=null){
			requestBuilder.setSessionId(sessionId);
		}
		ProtoUtil.setAttribute(requestBuilder, QueueMapping.FIELD_QUEUE_ID, queue.getId());
		ProtoUtil.setAttribute(requestBuilder, QueueMapping.FIELD_TOPIC_ID, queue.getTopic().getId());
		if(COMMAND_QUEUE_LIST.equals(serviceName)){
			ProtoUtil.setAttribute(requestBuilder, QueueMapping.PAGE_SIZE, pageSize);
			ProtoUtil.setAttribute(requestBuilder, QueueMapping.PAGE_NO, pageNo);
		}
		return requestBuilder.build();
	}

}
