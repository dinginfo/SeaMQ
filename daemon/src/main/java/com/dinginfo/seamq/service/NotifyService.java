package com.dinginfo.seamq.service;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.SocketContext;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.entity.mapping.SessionMapping;
import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.ResponseProto.ResponsePro;
import com.google.protobuf.ByteString;

import io.netty.channel.Channel;

public class NotifyService {	
	public static final String BEAN_NAME = "notifyService";
	
	private static final Logger logger = LogManager.getLogger(NotifyService.class);
	
	private static final int MAX_TASKS =100000;
	
	private static final int MAX_ITEMS =100000;
	
	private static final long TIME_OUT = 1000 * 60 * 2;
	
	private static final int CACHE_TIME_OUT = 60 * 10;

	private CachedMap<String, SocketContext> userSocketMap;
	
	private CachedMap<String, User> registerMap;
	
	private CachedMap<String, UserInfo> topicUserMap;
	
	private LinkedBlockingQueue taskQueue;
	
	private ExecutorService executorService;
	
	private TopicService topicService;
	
	private int poolSize = 10;
	
	public NotifyService(ServerConfig config){
		this.poolSize = config.getNotifyThreadPoolSize();
		init(100000);
	}
	
	public void setTopicService(TopicService topicService) {
		this.topicService = topicService;
	}
	
	private void init(int size){
	
		userSocketMap = new CachedMap<String,SocketContext>(MAX_ITEMS,CACHE_TIME_OUT);
		registerMap = new CachedMap<String,User>(MAX_ITEMS, CACHE_TIME_OUT);
		topicUserMap = new CachedMap<String,UserInfo>(MAX_ITEMS, CACHE_TIME_OUT);
		
		//
		taskQueue = new LinkedBlockingQueue();
		this.executorService = new ThreadPoolExecutor(poolSize, poolSize, 100, TimeUnit.SECONDS, taskQueue);
	}
	
	private void registerSocket(SocketContext context){
		StringBuilder sb = new StringBuilder(50);
		sb.append(String.valueOf(context.getDomainId()));
		sb.append(":");
		sb.append(context.getUser());
		String userId = sb.toString();
		SocketContext sc = userSocketMap.get(userId);
		if(sc!=null){
			if(sc.getChannelId().equals(context.getChannelId())){
				sc.setSessionId(context.getSessionId());
				sc.setUpdateTime(System.currentTimeMillis());
			}else{
				userSocketMap.put(userId, context);
			}
		}else{
			userSocketMap.put(userId, context);
		}
	}
	
	private void registerUser(String id,SocketContext context){
		User u = registerMap.get(id);
		if(u!=null){
			u.setUpdatedTime(Calendar.getInstance().getTime());
		}else{
			User user = new User();
			user.setName(context.getUser());
			user.setDomain(new MQDomain(context.getDomainId()));
			user.setUpdatedTime(Calendar.getInstance().getTime());
			registerMap.put(id, user);
		}
	}
	
	
	public void registerQueue(SocketContext context,MQueue queue){
		String regId = buildRegisterId(queue.getId(), context.getUser());
		registerUser(regId, context);
		registerSocket(context);
	}
	
	public void registerTopic(SocketContext context,MQTopic topic){
		String regId = buildRegisterId(topic.getId(), context.getUser());
		registerUser(regId, context);
		registerSocket(context);
	}
	
	private String buildRegisterId(String id,String user){
		StringBuilder sb = new StringBuilder(100); 
		sb.append(id);
		sb.append(":");
		sb.append(user);
		return sb.toString();
	}
	
	public void removeChannel(String channelId){
		if(channelId==null){
			return;
		}
		
		Map<String,SocketContext> contextMap = userSocketMap.getAll();
		String user = null;
		for(SocketContext context : contextMap.values()) {
			if(channelId.equals(context.getChannelId())){
				user = context.getUser();
				break;
			}
		}
		userSocketMap.remove(user);
	}
	
	public void clear(){
		userSocketMap.clear();
		registerMap.clear();
		topicUserMap.clear();
	}
	
	public void shutdown(){
		taskQueue.clear();
		if(executorService!=null){
			executorService.shutdown();	
		}
	}
	
	public void sendMessage(MQueue queue){
		if(taskQueue.size()>MAX_TASKS){
			return;
		}
		SendTask task = new SendTask(this, queue);
		executorService.submit(task);
	}
	
	private void send(MQueue queue){
		String topicId = queue.getTopic().getId();
		UserInfo info = topicUserMap.get(topicId);
		if(info==null){
			try {
				info = getUserList(topicId);
			} catch (Exception e) {
				StringWriter writer = new StringWriter();
				PrintWriter pw = new PrintWriter(writer);
				e.printStackTrace(pw);
				logger.error(writer.toString());
			}
			if(info==null){
				return;
			}
			info.setTime(System.currentTimeMillis());
			topicUserMap.put(topicId, info);
		}
		List<String> userList = info.getUserList();
		if(userList==null || userList.size()==0){
			return;
		}
		
		String id = null;
		User user  = null;
		User registerUser = null;
		Map<String,User> userMap= new HashMap<String, User>();
		for(String userName : userList){
			// for queue
			id = buildRegisterId(queue.getId(), userName);
			registerUser = registerMap.get(id);
			if(registerUser!=null) {
				userMap.put(userName, registerUser);
			}
			
			//for topic
			id = buildRegisterId(queue.getTopic().getId(), userName);
			registerUser = registerMap.get(id);
			if(registerUser!=null) {
				userMap.put(userName, registerUser);
			}
			
		}
		//send queue info
		SocketContext context = null;
		for(User u : userMap.values()) {
			context = userSocketMap.get(buildUserId(u));
			if(context==null){
				continue;
			}
			send(context, queue);
		}
	}

	
	private void send(SocketContext context,MQueue queue){
		Channel channel = context.getChannel();
		ResponsePro.Builder resBuilder = ResponsePro.newBuilder();
		resBuilder.setServiceName(Command.COMMAND_NOTIFY);
		DataField field = new DataField(MessageMapping.FIELD_TOPIC_NAME, queue.getTopic().getName());
		addField(resBuilder, field);
		field = new DataField(MessageMapping.FIELD_QUEUE, queue.getQueue());
		addField(resBuilder, field);
		field = new DataField(SessionMapping.FIELD_SID, context.getSessionId());
		addField(resBuilder, field);
		channel.writeAndFlush(resBuilder.build());
	}
	
	private void addField(ResponsePro.Builder resBuilder,DataField field) {
		DataFieldPro.Builder fieldBuilder = DataFieldPro.newBuilder();
		fieldBuilder.setKey(field.getKey());
		fieldBuilder.setDataType(field.getDataType());
		fieldBuilder.setData(ByteString.copyFrom(field.getData()));
		resBuilder.addFields(fieldBuilder);
	}
	
	private String buildUserId(User user){
		StringBuilder sb = new StringBuilder(50);
		sb.append(String.valueOf(user.getDomain().getId()));
		sb.append(":");
		sb.append(user.getName());
		return sb.toString();
	}
	
	private UserInfo getUserList(String topicId)throws Exception{
		UserInfo info = topicUserMap.get(topicId);
		if(info!=null){
			long time= System.currentTimeMillis() - info.getTime();
			if(time<TIME_OUT){
				return info;
			}
		}
		List<User> userList = topicService.getUserListByTopic(topicId);
		if(userList==null){
			return null;
		}
		List<String> users = new ArrayList<String>();
		for(User user : userList){
			users.add(user.getName());
		}
		if(users.size()==0){
			return null;
		}
		info = new UserInfo();
		info.setTime(System.currentTimeMillis());
		info.setUserList(users);
		topicUserMap.put(topicId, info);
		return info;
	}
	
	
	class UserInfo implements Serializable{
		private long time;
		
		private List<String> userList;

		public long getTime() {
			return time;
		}

		public void setTime(long time) {
			this.time = time;
		}

		public List<String> getUserList() {
			return userList;
		}

		public void setUserList(List<String> userList) {
			this.userList = userList;
		}
	}
	
	class SendTask implements Runnable{
		private NotifyService service;
		
		private MQueue queue;
		
		public SendTask(NotifyService service,MQueue queue){
			this.service = service;
			this.queue = queue;
		}
		
		@Override
		public void run() {
			service.send(queue);
		}
	}
	
}
