package com.dinginfo.seamq.service;

import com.dinginfo.seamq.entity.MQSession;

public interface SessionService {
	public static final String BEAN_NAME = "sessionService";

	public MQSession create(MQSession session)throws Exception;
	
	public int update(MQSession session)throws Exception;
	
	public int deleteByPK(String sessionId)throws Exception;
	
	public MQSession get(String sessionId,long timeout) throws Exception;
	
	public void remove(String sessionId);
	
	public void load()throws Exception;
	
	public int clearOldSession(long timeout)throws Exception;
	
	public void clear();
}
