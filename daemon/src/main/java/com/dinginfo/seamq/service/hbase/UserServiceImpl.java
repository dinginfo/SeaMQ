package com.dinginfo.seamq.service.hbase;

import java.util.List;

import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.service.UserService;
import com.dinginfo.seamq.storage.hbase.UserHBaseStorage;

public class UserServiceImpl implements UserService {
	
	private UserHBaseStorage userStorage =null;
	
	private boolean loaded = false;
	
	public UserServiceImpl(){
		
	}

	
	public void setUserStorage(UserHBaseStorage userStorage) {
		this.userStorage = userStorage;
	}


	@Override
	public void createUser(User user) throws Exception {
		if(user==null){
			throw new Exception("user is null");
		}
		userStorage.createUser(user);;
	}

	@Override
	public int updateUser(User user) throws Exception {
		if(user== null){
			throw new Exception("user is null");
		}
		return userStorage.updateUser(user);
	}

	@Override
	public int updateUserPassword(User user) throws Exception {
		if(user==null){
			throw new Exception("user is null");
		}
		return userStorage.updateUserPassword(user);
	}

	@Override
	public int deleteUserByPK(User user) throws Exception {
		if(user==null){
			throw new Exception("user is null");
		}
		return userStorage.deleteUserByPK(user);
	}

	@Override
	public User getUserByPK(User user) throws Exception {
		if(user==null){
			throw new Exception("user is null");
		}
		
		return userStorage.getUserByPK(user);		
	}

	@Override
	public User getUserByPK(String userObjID) throws Exception {
		return null;
	}

	@Override
	public String getPassword(User user) throws Exception {
		return userStorage.getPassword(user);
	}


	@Override
	public boolean isExist(User user) throws Exception {
		if(user==null){
			throw new Exception("user is null");
		}
		User u = userStorage.getUserByPK(user);
		if(u!=null){
			return true;
		}else{
			return false;
		}
	}

	@Override
	public long getUserCount(long domainId) throws Exception {
		int n =0;
		return userStorage.getUserCount(domainId);
	}

	@Override
	public List<User> getUserList(long domainId,int pageSize,int pageNo) throws Exception {
		return userStorage.getUserList(domainId, pageSize,pageNo);

	}
	

	@Override
	public void load() throws Exception {

	}


	@Override
	public void clear() {
		
		
	}

	

}
