package com.dinginfo.seamq.service.jdbc;

import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.storage.jdbc.SessionDBStorage;

public class SessionServiceImpl implements SessionService {
	private SessionDBStorage sessionStorage;
	

	public void setSessionStorage(SessionDBStorage sessionStorage) {
		this.sessionStorage = sessionStorage;
	}

	@Override
	public MQSession create(MQSession session) throws Exception {
		return sessionStorage.create(session);
	}

	@Override
	public int update(MQSession session) throws Exception {
		return sessionStorage.update(session);
	}

	@Override
	public int deleteByPK(String sessionId) throws Exception {
		return sessionStorage.deleteByPK(sessionId);
	}

	@Override
	public MQSession get(String sessionId, long timeout) throws Exception {
		MQSession session = sessionStorage.getByPK(sessionId);
		if(session==null) {
			return null;
		}
		long time = System.currentTimeMillis() -session.getUpdatedTime().getTime();
		if(time<timeout) {
			return session;
		}else {
			sessionStorage.deleteByPK(sessionId);
			return null;
		}
	}

	@Override
	public void remove(String sessionId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void load() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public int clearOldSession(long timeout) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

}
