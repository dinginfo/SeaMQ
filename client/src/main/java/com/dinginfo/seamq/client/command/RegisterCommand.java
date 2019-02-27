package com.dinginfo.seamq.client.command;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.common.ProtoUtil;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;

public class RegisterCommand extends AbstractCommand {
	private MQueue queue;
	
	private String type;
	
	private String congroupName;
	
	public RegisterCommand(MQueue queue,String type){
		this.queue = queue;
		this.type = type;
	}
	

	public String getCongroupName() {
		return congroupName;
	}


	public void setCongroupName(String congroupName) {
		this.congroupName = congroupName;
	}


	@Override
	public Object getSendObject() {
		RequestPro.Builder requestBuilder = RequestPro.newBuilder();
		requestBuilder.setServiceName(Command.COMMAND_REGISTER);
		requestBuilder.setRequestId(this.uuid);
		if(sessionId!= null){
			requestBuilder.setSessionId(sessionId);
		}
		if(type!=null){
			ProtoUtil.setField(requestBuilder, TopicMapping.REG_TYPE,type);
		}
		if(queue!=null && queue.getId()!=null) {
			ProtoUtil.setField(requestBuilder, MessageMapping.FIELD_QUEUE_ID,queue.getId());
		}
		ProtoUtil.setField(requestBuilder, MessageMapping.FIELD_CONSUMER_GROUP,congroupName);
		return requestBuilder.build();
	}

	

}
