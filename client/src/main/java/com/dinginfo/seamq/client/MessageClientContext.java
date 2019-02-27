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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.Command;


/** 
 * @author David Ding
 *
 */
public class MessageClientContext {
	private long connectionTimeout;
	
	private long operateTimeout;
	
	private List<NodeAddress> nodeList;
	
	private Map<String, Command> commandMap; 
	
	private String securityKey;
	
	private MessageNotifyHandler messageNotifyHandler;
	
	
	public void setNodes(List<String> nodes){
		nodeList=new ArrayList<NodeAddress>();
		NodeAddress node=null;
		for(String str: nodes){
			node=new NodeAddress();
			node.setAddress(str);
			nodeList.add(node);
		}
	}

	public long getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(long connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public List<NodeAddress> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<NodeAddress> nodeList) {
		this.nodeList = nodeList;
	}

	public Map<String, Command> getCommandMap() {
		return commandMap;
	}

	public void setCommandMap(Map<String, Command> commandMap) {
		this.commandMap = commandMap;
	}


	public String getSecurityKey() {
		return securityKey;
	}

	public void setSecurityKey(String securityKey) {
		this.securityKey = securityKey;
	}

	public long getOperateTimeout() {
		return operateTimeout;
	}

	public void setOperateTimeout(long operateTimeout) {
		this.operateTimeout = operateTimeout;
	}

	public MessageNotifyHandler getMessageNotifyHandler() {
		return messageNotifyHandler;
	}

	public void setMessageNotifyHandler(MessageNotifyHandler messageNotifyHandler) {
		this.messageNotifyHandler = messageNotifyHandler;
	}
	
}


