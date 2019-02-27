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
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.entity.mapping.UserMapping;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class UserCommand extends AbstractCommand {
	public static final int ACTION_GET=1;
	public static final int ACTION_CREATE=2;
	public static final int ACTION_UPDATE=3;
	public static final int ACTION_UPDATE_PASSWD=4;
	public static final int ACTION_DELETE=5;
	public static final int ACTION_COUNT=6;
	public static final int ACTION_LIST=7;
	
	protected String serviceName;
	
	protected User user;
	
	private int pageSize;
	
	private int pageNo;
	
	public UserCommand(int actionType,User user){
		this.user = user;
		switch(actionType){
		
		case ACTION_GET:
			serviceName = COMMAND_USER_GET;
			break;
		case ACTION_CREATE:
			serviceName = COMMAND_USER_CREATE;
			break;
		case ACTION_UPDATE:
			serviceName = COMMAND_USER_UPDATE;
			break;
		case ACTION_DELETE:
			serviceName = COMMAND_USER_DELETE;
			break;
		case ACTION_UPDATE_PASSWD:
			serviceName = COMMAND_USER_UPDATE_PASSWD;
			break;
		case ACTION_COUNT:
			serviceName = COMMAND_USER_COUNT;
			break;
		case ACTION_LIST:
			serviceName = COMMAND_USER_LIST;
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
		requestBuilder.setServiceName(serviceName);
		requestBuilder.setRequestId(this.uuid);
		if(sessionId!=null){
			requestBuilder.setSessionId(sessionId);
		}
		if(user==null){
			return requestBuilder.build();
		}
		
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_NAME, user.getName());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_PASSWD, user.getPwd());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_STATUS, user.getStatus());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_TYPE, user.getType());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_REMARK, user.getRemark());
		ProtoUtil.setAttribute(requestBuilder, DomainMapping.FIELD_DOMAIN_ID, user.getDomain().getId());
		if(COMMAND_USER_LIST.equals(serviceName)){
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_PAGE_SIZE, pageSize);
			ProtoUtil.setAttribute(requestBuilder, TopicMapping.FIELD_PAGE_NO, pageNo);
		}
		return requestBuilder.build(); 
	}
	
}
