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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.entity.MQTopic;


public class MQMessage implements Serializable{
	protected MQTopic topic;
	
	protected byte[] data;
	
	protected Map<String,DataField> attributeMap;

	public String getTopicName() {
		if(topic==null){
			return null;
		}
		return topic.getName();
	}

	public void setTopicName(String name) {
		topic = new MQTopic();
		topic.setName(name);
	}
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public String getMessageId(){
		return null;
	}
	
	public boolean isMessageArray(){
		return false;
	}
	
	public List<MQMessage> getMessageList(){
		return null;
	}

	public void putAttribute(String key,String value){
		if(attributeMap==null){
			attributeMap = new HashMap<String, DataField>();
		}
		DataField bean = new DataField(key, value);
		attributeMap.put(key, bean);
	}
	
	public List<DataField> getAttributeList(){
		if(attributeMap==null){
			return null;
		}else{
			List<DataField> list = new ArrayList<DataField>();
			for(DataField attribute : attributeMap.values()){
				list.add(attribute);
			}
			return list;
		}
		
	}

	
}
