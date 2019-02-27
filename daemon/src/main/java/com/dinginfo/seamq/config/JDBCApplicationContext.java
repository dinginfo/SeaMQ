package com.dinginfo.seamq.config;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.common.ApplicationContext;
import com.dinginfo.seamq.service.QueueAgentBuilder;
import com.dinginfo.seamq.service.QueueService;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.service.TopicService;
import com.dinginfo.seamq.service.UserService;
import com.dinginfo.seamq.service.jdbc.QueueAgentDBBuilder;
import com.dinginfo.seamq.service.jdbc.SessionServiceImpl;
import com.dinginfo.seamq.service.jdbc.TopicServiceImpl;
import com.dinginfo.seamq.service.jdbc.UserServiceImpl;
import com.dinginfo.seamq.storage.jdbc.MessageDBStorage;
import com.dinginfo.seamq.storage.jdbc.SessionDBStorage;
import com.dinginfo.seamq.storage.jdbc.TopicDBStorage;
import com.dinginfo.seamq.storage.jdbc.UserDBStorage;


public class JDBCApplicationContext extends ApplicationContext {
	
	public JDBCApplicationContext(){
		topicStorage();
		userStorage();
		sessionStorage();
		sessionStorage();
		messageStorage();
		dataSource();
		queueService();
		sessionService();
		topicService();
		userService();
		
	}

	
	
	public TopicDBStorage topicStorage(){
		TopicDBStorage topicStorage = getBean(TopicDBStorage.BEAN_NAME, TopicDBStorage.class);
		if(topicStorage!=null){
			return topicStorage;
		}
		topicStorage = new TopicDBStorage();
		topicStorage.setDataSource(dataSource());
		putBean(TopicDBStorage.BEAN_NAME, topicStorage);
		return topicStorage ;
	}
	

	public UserDBStorage userStorage(){
		UserDBStorage storage = getBean(UserDBStorage.BEAN_NAME, UserDBStorage.class);
		if(storage!=null){
			return storage;
		}
		UserDBStorage userStorage = new UserDBStorage();
		userStorage.setDataSource(dataSource());
		putBean(UserDBStorage.BEAN_NAME, userStorage);
		return userStorage;
	}
	

	public SessionDBStorage sessionStorage(){
		SessionDBStorage storage = getBean(SessionDBStorage.BEAN_NAME, SessionDBStorage.class);
		if(storage!=null){
			return storage;
		}
		SessionDBStorage sessionStorage =new SessionDBStorage();
		sessionStorage.setDataSource(dataSource());
		putBean(SessionDBStorage.BEAN_NAME,sessionStorage);
		return sessionStorage;
	}
	
	public MessageDBStorage messageStorage(){
		MessageDBStorage storage = getBean(MessageDBStorage.BEAN_NAME, MessageDBStorage.class);
		if(storage!=null){
			return storage;
		}
		MessageDBStorage messageStorage =new MessageDBStorage();
		messageStorage.setDataSource(dataSource());
		putBean(MessageDBStorage.BEAN_NAME, messageStorage);
		return messageStorage;
	}
	

	public QueueAgentDBBuilder queueAgentBuilder(){
		QueueAgentDBBuilder builder = getBean(QueueAgentBuilder.BEAN_NAME, QueueAgentDBBuilder.class);
		if(builder!=null) {
			return builder;
		}
		builder = new QueueAgentDBBuilder();
		builder.setMessageStorage(messageStorage());
		builder.setTopicStorage(topicStorage());
		putBean(QueueAgentBuilder.BEAN_NAME, builder);
		return builder;
	}
	

	public QueueService queueService(){
		QueueService service = getBean(QueueService.BEAN_NAME, QueueService.class);
		if(service!=null){
			return service;
		}
		service  = new QueueService();
		service.setTopicService(topicService());
		service.setQueueAgentBuilder(queueAgentBuilder());
		putBean(QueueService.BEAN_NAME, service);
		return service;
	}
	
	public SessionService sessionService() {
		SessionServiceImpl service = getBean(SessionService.BEAN_NAME, SessionServiceImpl.class);
		if(service!=null) {
			return service;
		}
		service = new SessionServiceImpl();
		service.setSessionStorage(sessionStorage());
		putBean(SessionService.BEAN_NAME, service);
		return service;
	}
	
	public TopicService topicService() {
		TopicServiceImpl service = getBean(TopicService.BEAN_NAME, TopicServiceImpl.class);
		if(service!=null) {
			return service;
		}
		service = new TopicServiceImpl();
		service.setTopicStorage(topicStorage());
		putBean(TopicService.BEAN_NAME, service);
		return service;
	}
	
	public UserService userService() {
		UserServiceImpl service = getBean(UserService.BEAN_NAME, UserServiceImpl.class);
		if(service!=null) {
			return service;
		}
		service = new UserServiceImpl();
		service.setUserStorage(userStorage());
		putBean(UserService.BEAN_NAME, service);
		return service;
	}

	public DataSource dataSource(){
		String beanName="dataSource";
		DataSource ds = getBean(beanName, DataSource.class);
		if(ds!=null){
			return ds;
		}
		ServerConfig config = new ServerConfig();
		
		
		String className =config.getJDBCClassName();
		String url =config.getJDBCUrl();
		String userName =config.getJDBCUserName();
		String password = config.getJDBCPassword();
		
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(className);
		dataSource.setUrl(url);
		dataSource.setUsername(userName);
		dataSource.setPassword(password);
		dataSource.setMaxTotal(config.getJDBCMaxPoolSize());
		try {
			Connection conn = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		putBean(beanName, dataSource);
		return dataSource;
	}
	
}
