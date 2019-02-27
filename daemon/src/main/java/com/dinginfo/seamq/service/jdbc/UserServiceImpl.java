package com.dinginfo.seamq.service.jdbc;

import java.util.List;

import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.service.UserService;
import com.dinginfo.seamq.storage.jdbc.UserDBStorage;

public class UserServiceImpl implements UserService {
	private UserDBStorage userStorage;

	public void setUserStorage(UserDBStorage userStorage) {
		this.userStorage = userStorage;
	}

	@Override
	public void createUser(User user) throws Exception {
		userStorage.createUser(user);
	}

	@Override
	public int updateUser(User user) throws Exception {
		return userStorage.updateUser(user);
	}

	@Override
	public int updateUserPassword(User user) throws Exception {
		return userStorage.updateUserPassword(user);
	}

	@Override
	public int deleteUserByPK(User user) throws Exception {
		return userStorage.deleteUserByPK(user);
	}

	@Override
	public User getUserByPK(User user) throws Exception {
		return userStorage.getUserByPK(user);
	}

	@Override
	public User getUserByPK(String userObjID) throws Exception {
		return userStorage.getUserByPK(userObjID);
	}


	@Override
	public String getPassword(User user) throws Exception {
		return userStorage.getPassword(user);
	}

	@Override
	public boolean isExist(User user) throws Exception {
		User u = getUserByPK(user);
		if(u!=null) {
			return true;
		}else {
			return false;	
		}
	}

	@Override
	public long getUserCount(long domainId) throws Exception {
		return userStorage.getUserCount(domainId);
	}


	@Override
	public List<User> getUserList(long domainId, int pageNo, int pageSize) throws Exception {
		return userStorage.getUserList(domainId, pageNo, pageSize);
	}

	@Override
	public void load() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

}
