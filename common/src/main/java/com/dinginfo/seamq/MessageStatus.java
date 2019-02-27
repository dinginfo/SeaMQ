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

public class MessageStatus {
	public static final int OK = 0;
	
	public static final int ERROR=1;
	
	public static final int ERR_OFFSET=2;
	
	public static final int EXIST_MESSAGE=3;
	
	public static final int NOT_MESSAGE=4;
	
	private int status = -1;
	
	public MessageStatus(int msgStatus){
		this.status = msgStatus;
	}

	public int getStatus() {
		return status;
	}

}
