package com.dinginfo.seamq.service;

import java.util.List;

import com.dinginfo.seamq.entity.User;

public interface UserService {
	public static final String BEAN_NAME="userService";
	
	public void createUser(User user) throws Exception;

	public int updateUser(User user) throws Exception;

	public int updateUserPassword(User user) throws Exception;
	
	public String getPassword(User user)throws Exception;

	public int deleteUserByPK(User user) throws Exception;

	public User getUserByPK(User user) throws Exception;
	
	public User getUserByPK(String userObjID) throws Exception;
	
	public boolean isExist(User user)throws Exception;
	
	public long getUserCount(long domainId)throws Exception;
	
	public List<User> getUserList(long domainId,int pageNo,int pageSize)throws Exception;
	
	public void load()throws Exception;
	
	public void clear();
}
