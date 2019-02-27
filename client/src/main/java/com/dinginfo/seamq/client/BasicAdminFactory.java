package com.dinginfo.seamq.client;

import com.dinginfo.seamq.CachedMap;

public abstract class BasicAdminFactory {
	private static final int  MAX_ITEM_SIZE= 10000;
	
	private  MQClientService clientService = null;
	
	private  CachedMap<String,MQAdmin> adminMap= null;
	
	protected synchronized void createClientService(MQConfig config) throws Exception{
		if(clientService!=null){
			return;
		}
		clientService = new MQClientService(config);
		clientService.init();
		clientService.subscribeBrokers();
	}
	
	protected MQClientService getClientService(){
		return clientService;
	}
	
	public void init(){
		adminMap= new CachedMap<>(MAX_ITEM_SIZE);
	}
	
	protected void putMQAdmin(String sessionId,MQAdmin admin){
		adminMap.put(sessionId, admin);
	}
	
	public MQAdmin createMQAdmin(String user, String pwd, MQConfig config)throws Exception {
		if(clientService==null){
			createClientService(config);
		}
		
		MQAdminImpl admin = new MQAdminImpl(clientService); 
		String sid = admin.login(0L,user, pwd, admin.getMasterClient());
		if(sid!=null && sid.trim().length()>0){
			adminMap.put(sid, admin);
			return admin;
		}else{
			return null;
		}
	}
	
	public MQAdmin getMQAdmin(String sessionId){
		return adminMap.get(sessionId);
	}
	
	public void removeMQAdmin(String sessionId){
		adminMap.remove(sessionId);
	}
	
	public void shutdown(){
		if(clientService!=null){
			clientService.shutdown();
		}
	}

}
