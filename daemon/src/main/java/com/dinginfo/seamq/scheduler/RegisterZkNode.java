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
package com.dinginfo.seamq.scheduler;

import org.I0Itec.zkclient.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class RegisterZkNode implements Runnable {
	private final static Logger logger = LogManager.getLogger(RegisterZkNode.class);
	private String path;
	
	private String data;
	
	private ZkClient zkClient;
	
	private boolean running;
	
	public RegisterZkNode(ZkClient zkClient,String path,String data){
		this.zkClient = zkClient;
		this.path = path;
		this.data = data;
		this.running = false;
	}

	@Override
	public void run() {

		if(logger.isDebugEnabled()){
			StringBuilder sb = new StringBuilder();
			sb.append("start register zookeeper node,path:");
			sb.append(path);
			logger.debug(sb.toString());
		}
		if(running){
			return ;
		}
		try{
			running = true;
			if(!zkClient.exists(path)){
				zkClient.createEphemeral(path,data);
				if(logger.isDebugEnabled()){
					StringBuilder sb = new StringBuilder();
					sb.append("register zookeeper node,path:");
					sb.append(path);
					logger.debug(sb.toString());
				}
			}
		}finally{
			running = false;
		}
	}

}
