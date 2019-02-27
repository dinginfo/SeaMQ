package com.dinginfo.seamq.config;

import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.common.ApplicationContext;
import com.dinginfo.seamq.service.QueueService;
import com.dinginfo.seamq.service.SessionService;
import com.dinginfo.seamq.service.TopicService;
import com.dinginfo.seamq.service.UserService;
import com.dinginfo.seamq.service.hbase.QueueAgentHBaseBuilder;
import com.dinginfo.seamq.service.hbase.SessionServiceImpl;
import com.dinginfo.seamq.service.hbase.TopicServiceImpl;
import com.dinginfo.seamq.service.hbase.UserServiceImpl;
import com.dinginfo.seamq.storage.hbase.HBaseDataSource;
import com.dinginfo.seamq.storage.hbase.MessageHBaseStorage;
import com.dinginfo.seamq.storage.hbase.SessionHBaseStorage;
import com.dinginfo.seamq.storage.hbase.TopicHBaseStorage;
import com.dinginfo.seamq.storage.hbase.UserHBaseStorage;


public class HBaseApplicationContext extends ApplicationContext {
	
	public HBaseApplicationContext(){
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

	
	
	public TopicHBaseStorage topicStorage(){
		TopicHBaseStorage topicStorage = getBean(TopicHBaseStorage.BEAN_NAME, TopicHBaseStorage.class);
		if(topicStorage!=null){
			return topicStorage;
		}
		ServerConfig config = new ServerConfig();
		String namespace = config.getHBaseNamespace();
		topicStorage = new TopicHBaseStorage(namespace);
		topicStorage.setDataSource(dataSource());
		putBean(TopicHBaseStorage.BEAN_NAME, topicStorage);
		return topicStorage ;
	}
	

	public UserHBaseStorage userStorage(){
		UserHBaseStorage userStorage = getBean(UserHBaseStorage.BEAN_NAME, UserHBaseStorage.class);
		if(userStorage!=null){
			return userStorage;
		}
		ServerConfig config = new ServerConfig();
		String namespace = config.getHBaseNamespace();
		userStorage = new UserHBaseStorage(namespace);
		userStorage.setDataSource(dataSource());
		putBean(UserHBaseStorage.BEAN_NAME, userStorage);
		return userStorage;
	}
	

	public SessionHBaseStorage sessionStorage(){
		SessionHBaseStorage sessionStorage = getBean(SessionHBaseStorage.BEAN_NAME, SessionHBaseStorage.class);
		if(sessionStorage!=null){
			return sessionStorage;
		}
		ServerConfig config = new ServerConfig();
		String namespace = config.getHBaseNamespace();
		sessionStorage =new SessionHBaseStorage(namespace);
		sessionStorage.setDataSource(dataSource());
		putBean(SessionHBaseStorage.BEAN_NAME,sessionStorage);
		return sessionStorage;
	}
	
	public MessageHBaseStorage messageStorage(){
		MessageHBaseStorage messageStorage = getBean(MessageHBaseStorage.BEAN_NAME, MessageHBaseStorage.class);
		if(messageStorage!=null){
			return messageStorage;
		}
		ServerConfig config = new ServerConfig();
		String namespace = config.getHBaseNamespace();
		messageStorage =new MessageHBaseStorage(namespace);
		messageStorage.setDataSource(dataSource());
		putBean(MessageHBaseStorage.BEAN_NAME, messageStorage);
		return messageStorage;
	}
	

	public QueueAgentHBaseBuilder queueAgentBuilder(){
		QueueAgentHBaseBuilder builder = getBean(QueueAgentHBaseBuilder.BEAN_NAME, QueueAgentHBaseBuilder.class);
		if(builder!=null) {
			return builder;
		}
		builder = new QueueAgentHBaseBuilder();
		builder.setMessageStorage(messageStorage());
		builder.setTopicStorage(topicStorage());
		putBean(QueueAgentHBaseBuilder.BEAN_NAME, builder);
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

	public HBaseDataSource dataSource(){
		
		HBaseDataSource ds = getBean(HBaseDataSource.BEAN_NAME, HBaseDataSource.class);
		if(ds!=null){
			return ds;
		}
		ServerConfig config = new ServerConfig();
		ds = new HBaseDataSource();
		ds.setZkPort(config.getHBaseZookeeperClientPort());
		ds.setZkQuorum(config.getHBaseZookeeperQuorum());
		putBean(HBaseDataSource.BEAN_NAME, ds);
		return ds;
	}
	
}
