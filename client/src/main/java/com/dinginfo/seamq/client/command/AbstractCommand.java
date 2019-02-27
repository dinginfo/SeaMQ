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

import com.dinginfo.seamq.MQMessage;
import com.dinginfo.seamq.MQResponse;

/** 
 * @author David Ding
 *
 */
public abstract class AbstractCommand implements ClientCommand{
	
	protected CountDownLatch latch;
	
	protected String uuid;
	
	protected MQResponse result;
	
	protected String status;
	
	protected boolean decoded;
	
	protected String securityKey;
	
	protected String sessionId;
		
	public AbstractCommand(){
		
		//latch=new CountDownLatch(1);
	}
	
	public void resetCountDownLatch(){
		latch=new CountDownLatch(1);
	}
	
	public void setUuid(String uuid){
		this.uuid=uuid;
	}
	
	public String getUuid() {
		return uuid;
	}
	
	public void countDown(){
		if(latch!=null){
			latch.countDown();
		}
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

	
	public String getStatus(){
		return this.status;
	}

	public void setResult(MQResponse result) {
		this.result = result;
	}
	
	public MQResponse getResult() {
		if(!decoded){
			decode();
		}
		return result;
	}
	
	public boolean checkResult(){
		return false;
	}
	
	public String getSecurityKey() {
		return securityKey;
	}

	public void setSecurityKey(String securityKey) {
		this.securityKey = securityKey;
	}


	public String getSessionId() {
		return sessionId;
	}

	@Override
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public void decode(){
			
	}
	

	@Override
	public abstract Object getSendObject();
	
	
}
