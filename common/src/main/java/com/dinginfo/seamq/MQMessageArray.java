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
import java.util.List;

import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.MessageArrayProto.MessageArrayPro;
import com.dinginfo.seamq.protobuf.MessageProto.MessagePro;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MQMessageArray implements Serializable{
	private List<MQMessage> messageList ;
	
	public MQMessageArray(){
		this.messageList = new ArrayList<MQMessage>();
	}
	
	public MQMessageArray(byte[] data){
		messageList = new ArrayList<MQMessage>();
		buildMessage(data);
	}
	
	public void put(MQMessage message){
		messageList.add(message);
	}

	public void buildMessage(byte[] data){
		if(data==null || data.length==0){
			return;
		}
		MessageArrayPro.Builder msgArrayBuilder = MessageArrayPro.newBuilder();
		try {
			msgArrayBuilder.mergeFrom(data);
			MessageArrayPro msgArray = msgArrayBuilder.build();
			List<MessagePro> msgProList = msgArray.getMessagesList();
			if(msgProList==null || msgProList.size()==0){
				return;
			}
			MQMessage message = null;
			List<DataFieldPro> attProList = null;
			for(MessagePro msgPro : msgProList){
				message = new MQMessage();
				message.setData(msgPro.getData().toByteArray());
				attProList=msgPro.getAttributesList();
				if(attProList!=null && attProList.size()>0){
					for(DataFieldPro attPro : attProList){
						message.putAttribute(attPro.getKey(), attPro.getData().toString());
					}
				}
				messageList.add(message);
			}
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			
		}
		
	}
	
	public byte[] toBytes(){
		if(messageList==null || messageList.size()==0){
			return null;
		}
		MessageArrayPro.Builder msgArrayBuilder = MessageArrayPro.newBuilder();
		MessagePro.Builder msgBuilder = null;
		DataFieldPro.Builder attBuilder = null;
		List<DataField> attList = null;
		for(MQMessage msg : messageList){
			msgBuilder = MessagePro.newBuilder();
			msgBuilder.setData(ByteString.copyFrom(msg.getData()));
			attList = msg.getAttributeList();
			if(attList!=null){
				for(DataField attBean : attList){
					attBuilder = DataFieldPro.newBuilder();
					attBuilder.setKey(attBean.getKey());
					attBuilder.setDataType(attBean.getDataType());
					attBuilder.setData(ByteString.copyFrom(attBean.getData()));
					msgBuilder.addAttributes(attBuilder.build());
				}
			}
			msgArrayBuilder.addMessages(msgBuilder.build());
		}
		return msgArrayBuilder.build().toByteArray();
	}
	
	public List<MQMessage> getMessageList() {
		return messageList;
	}
	
	public static void main(String[] args){
		MQMessageArray msgArray = new MQMessageArray();
		MQMessage msg = null;
		String str = null;
		for(int i=0;i<100;i++){
			msg = new MQMessage();
			str = "data"+String.valueOf(i);
			msg.setData(str.getBytes());
			for(int j=0;j<1;j++){
				msg.putAttribute("key"+j, "value"+j);
			}
			msgArray.put(msg);
		}
		byte[] msgData = msgArray.toBytes();
		System.out.println("data size:"+msgData.length);
		MQMessageArray msgArray2 = new MQMessageArray(msgData);
		List<MQMessage> msgList = msgArray2.getMessageList();
		for(MQMessage message : msgList){
			System.out.println(new String(message.getData()));
		}
		System.out.println(msgArray2);
	}
}
