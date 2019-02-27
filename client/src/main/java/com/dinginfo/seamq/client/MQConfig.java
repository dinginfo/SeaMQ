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

package com.dinginfo.seamq.client;

public class MQConfig {
	private static final int brokerBucketSize =10; 
	
	private String zookeeperURL;
	
	private String mqRoot;
	
	private long operateTimeout;
	
	private int messageRepeatNum;
	
	private int connectionRepeatNum;

	public String getZookeeperURL() {
		return zookeeperURL;
	}

	public void setZookeeperURL(String zookeeperURL) {
		this.zookeeperURL = zookeeperURL;
	}

	public String getMQRoot() {
		if(mqRoot!=null){
			String root = mqRoot.replace("/", "");
			return root;
		}else{
			return "SeaMQ";
		}
	}

	public void setMQRoot(String mqRoot) {
		this.mqRoot = mqRoot;
	}

	public int getBrokerBucketSize() {
		return brokerBucketSize;
	}
	
	public long getOperateTimeout() {
		return operateTimeout;
	}

	public void setOperateTimeout(long operateTimeout) {
		this.operateTimeout = operateTimeout;
	}
	
	public int getMessageRepeatNum() {
		return messageRepeatNum;
	}

	public void setMessageRepeatNum(int messageRepeatNum) {
		this.messageRepeatNum = messageRepeatNum;
	}

	public int getConnectionRepeatNum() {
		return connectionRepeatNum;
	}

	public void setConnectionRepeatNum(int connectionRepeatNum) {
		this.connectionRepeatNum = connectionRepeatNum;
	}

	public long getMQObjectTimeout(){
		return 2 * 60 * 1000;
	}
	
	
}
