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

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.entity.NodeInfo;

public class ZKUtils {
	
	public static List<NodeInfo> getBrokerNodeInfos(String zkurl,String rootPath,int brokerBucketSize)throws Exception{
		List<NodeInfo> list = null;
		ZkClient zkclient = new ZkClient(zkurl);
		try{
			list = getBrokerNodeInfos(zkclient, rootPath,brokerBucketSize);
		}catch(Exception e){
			throw e;
		}finally{
			zkclient.close();
		}
		
		return list;
	}
	
	public static NodeInfo getMasterNodeInfo(ZkClient zkclient,String rootPath)throws Exception{
		NodeInfo result =null;
		StringBuilder sb =new StringBuilder(100);
		sb.append("/");
		sb.append(rootPath);
		sb.append("/master");
		String path =sb.toString();
		if(zkclient.exists(path)){
			String jsonString = zkclient.readData(path);
			result  = JSON.parseObject(jsonString, NodeInfo.class);
		}
		return result;
	}
	
	public static NodeInfo getMonitorNodeInfo(ZkClient zkclient,String rootPath)throws Exception{
		NodeInfo result =null;
		StringBuilder sb =new StringBuilder(100);
		sb.append("/");
		sb.append(rootPath);
		sb.append("/monitor");
		String path =sb.toString();
		if(zkclient.exists(path)){
			String jsonString = zkclient.readData(path);
			result  = JSON.parseObject(jsonString, NodeInfo.class);
		}
		return result;
	}
	
	public static List<NodeInfo> getBrokerNodeInfos(ZkClient zkclient,String rootPath,int brokerBucketSize)throws Exception{
		List<NodeInfo> resultList = new ArrayList<NodeInfo>();
		StringBuilder sb = new StringBuilder(100); 
		sb.append("/");
		sb.append(rootPath);
		sb.append("/brokers/");
		String brokerPath = sb.toString();
		String bucketPath = null;
		List<String> brokerList=null;
		String jsonString =null;
		NodeInfo node =null;
		for(int i=0;i<brokerBucketSize;i++){
			sb = new StringBuilder(50);
			sb.append(brokerPath);
			sb.append(String.valueOf(i));
			bucketPath = sb.toString();
			if(!zkclient.exists(bucketPath)){
				continue;
			}
			brokerList= zkclient.getChildren(bucketPath);
			if(brokerList==null || brokerList.size()==0){
				continue;
			}
			for(String nodeName : brokerList){
				sb = new StringBuilder(100);
				sb.append(bucketPath);
				sb.append("/");
				sb.append(nodeName);
				jsonString = zkclient.readData(sb.toString());
				if(jsonString==null || jsonString.trim().length()==0){
					continue;
				}
				node = JSON.parseObject(jsonString, NodeInfo.class);
				resultList.add(node);
			}
		}
		return resultList;
	}
	
	public static NodeInfo getBrokerNodeInfo(ZkClient zkclient,String rootPath,int brokerBucketSize,String nodeId)throws Exception{
		StringBuilder sb = new StringBuilder(100); 
		sb.append("/");
		sb.append(rootPath);
		sb.append("/brokers/");
		int k = nodeId.hashCode() % brokerBucketSize;
		if(k<0){
			k = Math.abs(k);
		}
		sb.append(String.valueOf(k));
		sb.append("/");
		sb.append(nodeId);
		String brokerPath = sb.toString();
		String jsonString =null;
		NodeInfo node =null;
		if(!zkclient.exists(brokerPath)){
			return null;
		}
		jsonString = zkclient.readData(sb.toString());
		if(jsonString==null){
			return null;
		}else{
			node =JSON.parseObject(jsonString, NodeInfo.class);
			 return node;
		}	
	}
}
