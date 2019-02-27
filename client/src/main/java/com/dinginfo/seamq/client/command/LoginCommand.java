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

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.common.ProtoUtil;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.DomainMapping;
import com.dinginfo.seamq.entity.mapping.UserMapping;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class LoginCommand extends AbstractCommand {
	private User user;
	
	public LoginCommand(User user){
		this.user = user;
	}

	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(Command.COMMAND_LOGIN);
		requestBuilder.setRequestId(this.uuid);
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_NAME, user.getName());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_PASSWD, user.getPwd());
		ProtoUtil.setAttribute(requestBuilder, UserMapping.FIELD_TYPE, user.getType());
		ProtoUtil.setAttribute(requestBuilder, DomainMapping.FIELD_DOMAIN_ID, user.getDomain().getId());
		return requestBuilder.build();
	}

}
