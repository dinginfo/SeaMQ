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

package com.dinginfo.seamq.service.hbase;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.storage.hbase.SessionHBaseStorage;

public class SessionServiceImpl implements SessionService{
	
	private static final int ITEM_SIZE=500000;

	private SessionHBaseStorage sessionStorage;

	private CachedMap<String, MQSession> sessionMap ;
	
	private boolean running=false;
	
	private boolean loaded = false;
	
	public SessionServiceImpl(){
		this.sessionMap = new CachedMap<String, MQSession>(ITEM_SIZE);
	}
	

	public void setSessionStorage(SessionHBaseStorage sessionStorage) {
		this.sessionStorage = sessionStorage;
	}


	@Override
	public MQSession create(MQSession session)throws Exception{
		if(session == null){
			throw new Exception("session is null");
		}
		MQSession s = sessionStorage.create(session);
		if(s!=null){
			sessionMap.put(s.getId(), s);
		}
		return s; 
	}
	
	@Override
	public int update(MQSession session)throws Exception{
		if(session == null){
			return 0;
		}
		int n = sessionStorage.update(session);
		if(n>0){
			MQSession s = sessionMap.get(session.getId());
			if(s!=null){
				s.setUpdatedTime(Calendar.getInstance().getTime());
			}
		}
		
		return n;
	}
	
	@Override
	public int deleteByPK(String sessionId)throws Exception{
		sessionMap.remove(sessionId);
		return sessionStorage.deleteByPK(sessionId);
	}

	@Override
	public MQSession get(String sessionId,long timeout) throws Exception {
		MQSession session = sessionMap.get(sessionId);
		if(session==null){
			session = sessionStorage.getByPK(sessionId);
		}
		if(!isTimeout(session, timeout)){
			Date updatedTime = new Date(System.currentTimeMillis());
			session.setUpdatedTime(updatedTime);
			return session;
		}else{
			sessionMap.remove(sessionId);
		}
		session = sessionStorage.getByPK(sessionId);
		if(isTimeout(session, timeout)){
			return null;
		}
		Date updatedTime = new Date(System.currentTimeMillis());
		session.setUpdatedTime(updatedTime);
		sessionMap.put(sessionId, session);
		return session;
	}
	
	
	private boolean isTimeout(MQSession session, long timeout) {
		if(session == null){
			return true;
		}
		long timeInterval = System.currentTimeMillis()
				- session.getUpdatedTime().getTime();
		if (timeInterval > timeout) {
			return true;
		} else {
			return false;
		}

	}

	@Override
	public void remove(String sessionId) {
		sessionMap.remove(sessionId);
	}

	@Override
	public void load() throws Exception {	
		loaded = true;
	}


	@Override
	public void clear() {
		sessionMap.clear();
		loaded = false;
	}
	
	@Override
	public int clearOldSession(long timeout)throws Exception{
		if(running){
			return 0;
		}
		try{
			running=true;
			int result =0;
			Map<String,MQSession> map = sessionMap.getAll();
			long time = System.currentTimeMillis();
			long timeInterval = 0;
			List<MQSession> sessionList = new ArrayList<MQSession>();
			for(MQSession session : map.values()){
				timeInterval = session.getUpdatedTime().getTime() - time;
				if(timeInterval>0){
					sessionMap.remove(session.getId());
					sessionStorage.deleteByPK(session.getId());
					result ++;
				}else{
					sessionList.add(session);
				}
			}
			return result;	
		}finally{
			running=false;
		}
	}
	
	private List<MQSession> getSessionList(){
		Map<String,MQSession> map = sessionMap.getAll();
		if(map==null){
			return null;
		}
		List<MQSession> sessionList  = new ArrayList<MQSession>();
		for(MQSession session : map.values()){
			sessionList.add(session);
		}
		return sessionList;
	}
}
