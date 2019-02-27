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
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class UpdateSessionCommand extends AbstractCommand {
	
	public UpdateSessionCommand(String sessionId){
		this.sessionId = sessionId;
	}
	

	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(Command.COMMAND_UPDATE_SESSION);
		requestBuilder.setRequestId(this.uuid);
		requestBuilder.setSessionId(sessionId);
		return requestBuilder.build();
	}

}
