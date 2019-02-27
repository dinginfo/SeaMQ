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

import java.util.concurrent.CountDownLatch;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.MQResponse;

public interface ClientCommand extends Command {
	
	public CountDownLatch getLatch();

	public void countDown();

	public void resetCountDownLatch();

	public Object getSendObject();

	public void setResult(MQResponse result);

	public MQResponse getResult();

	public void decode();

	public String getStatus();

	public void setUuid(String uuid);

	public String getUuid();

	public void setSecurityKey(String key);
	
	public void setSessionId(String sessionId);
}
