package com.dinginfo.seamq;

import io.netty.channel.Channel;

public class SocketContext {
	private Channel channel;
	
	private String channelId;
	
	private String user;
	
	private long domainId;
	
	private long updateTime;
	
	private String sessionId;

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		if(channel==null){
			return;
		}
		this.channel = channel;
		this.channelId = channel.id().toString(); 
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getChannelId() {
		return channelId;
	}

	public long getDomainId() {
		return domainId;
	}

	public void setDomainId(long domainId) {
		this.domainId = domainId;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	
}
