package com.dinginfo.seamq.init;

import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.service.UserService;

public class InitAdminUser {
	private static final Logger logger = LogManager.getLogger(InitAdminUser.class);
	
	private UserService userService=null;
	
	public InitAdminUser(){
		userService = MyBean.getBean(UserService.BEAN_NAME, UserService.class);
	}
	
	public void initAdmin(String psw){ 
		User user = new User("admin");
		user.setType(User.TYPE_ADMIN);
		user.setDomain(new MQDomain(0));
		try {
			User admin = userService.getUserByPK(user);
			if(admin ==null){
				create(psw);
			}else{
				update(psw);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		
	}
	
	private void create(String psw)throws Exception{
		User user= new User();
		user.setName("admin");
		user.setDomain(new MQDomain(0));
		user.setType(User.TYPE_ADMIN);
		user.buildUserObjectID();
		user.setPwd(StringUtil.generateSHA512String(psw));
		Date date = Calendar.getInstance().getTime();
		user.setCreatedTime(date);
		user.setUpdatedTime(date);
		userService.createUser(user);
		logger.info("create admin user susscess");
	}
	
	private void update(String psw)throws Exception{
		User user= new User();
		user.setName("admin");
		user.setDomain(new MQDomain(0));
		user.setType(User.TYPE_ADMIN);
		user.buildUserObjectID();
		user.setPwd(StringUtil.generateSHA512String(psw));
		Date date = Calendar.getInstance().getTime();
		user.setUpdatedTime(date);
		userService.updateUserPassword(user);
		logger.info("update admin password susscess");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String psw ="admin";
		if(args!=null && args.length>0){
			psw = args[0];
		}
		InitAdminUser init = new InitAdminUser();
		init.initAdmin(psw);

	}

}
