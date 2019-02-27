package com.dinginfo.seamq.client;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.client.command.RegisterCommand;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.mapping.TopicMapping;

public class MessageListenService implements MessageNotifyHandler{
	private static final Logger logger = LogManager.getLogger(MessageListenService.class);
	
	private static MessageListenService instance; 
	
	private static final int MAX_ITEMS = 1000 * 100;
	
	private int maxQueueSize =1000 * 1000;

	private boolean registed = false;
	
	private MQClientService clientService;
	
	private Map<String, MQConsumer> consumerMap;
	
	private Map<String,MessageListenerContext> contextMap;
	
	private LinkedBlockingQueue taskQueue;
	
	private ExecutorService executorService;
	
	private ScheduledExecutorService scheduledService ;
	
	private RegistedTask registedTask;
	
	private int scheduledPeriod = 60 * 2;
	
	private int poolSize = 100;
	
	private boolean running = false;
	
	public static MessageListenService getInstance(){
		if(instance == null){
			createInstance();
		}
		return instance;
	}
	
	private static synchronized void createInstance(){
		if(instance!=null){
			return;
		}
		instance = new MessageListenService();
		
	}
	
	private MessageListenService(){
		consumerMap = new HashMap<String, MQConsumer>();
		contextMap = new HashMap<String,MessageListenerContext>();
		taskQueue = new LinkedBlockingQueue();
		
	}

	public void init() {
		shutdown();
		this.executorService = new ThreadPoolExecutor(poolSize, poolSize, 100, TimeUnit.SECONDS, taskQueue);
		registedTask = new RegistedTask(this);
		scheduledService = Executors.newScheduledThreadPool(1);
		scheduledService.scheduleAtFixedRate(registedTask, 60, scheduledPeriod, TimeUnit.SECONDS);
	}
	
	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	public void setScheduledPeriod(int scheduledPeriod) {
		if(scheduledPeriod>0) {
			this.scheduledPeriod = scheduledPeriod;	
		}
	}

	public void setPoolSize(int poolSize) {
		if(poolSize>0) {
			this.poolSize = poolSize;	
		}
	}

	public void shutdown() {
		if(scheduledService!=null) {
			scheduledService.shutdown();
		}
		if(executorService!=null) {
			executorService.shutdown();
		}
	}
	
	@Override
	public void doHandle(MessageNotifyContext context) {
		if(taskQueue.size()>maxQueueSize) {
			return;
		}
		MessageTask task = new MessageTask(this, context);
		executorService.submit(task);
	}
	
	
	private void doTask(MessageNotifyContext context) {
		MQConsumer consumer= consumerMap.get(context.getSessionId());
		if(consumer==null){
			return ;
		}
	
		int queue = context.getQueue();
		MessageListenerContext bean = new MessageListenerContext(consumer, context.getTopicName(), queue);
		MessageListenerContext listenContext = contextMap.get(bean.getId());
		MessageListener listener = null;
		MessageContext mc = new MessageContext();
		if(listenContext!=null){
			mc.setQueue(queue);
		}else{
			mc.setQueue(-1);
			bean = new MessageListenerContext(consumer, context.getTopicName());
			listenContext = contextMap.get(bean.getId());
		}
		
		if(listenContext==null) {
			return;
		}
		mc.setConsumer(consumer);
		mc.setTopicName(context.getTopicName());
		Map<String,ConsumerGroupContext> groupContextMap = listenContext.getCongroupMap();
		for(ConsumerGroupContext gcontext : groupContextMap.values()) {
			listener = gcontext.listener;
			listener.doHandle(mc);
		}
	}
	
	public void addListener(MessageListener listener,MQConsumer consumer,String topicName,int queueIdx)throws Exception{
		String consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		addListener(listener, consumer, topicName, queueIdx, consumerGroupName);
	}
	
	public void addListener(MessageListener listener,MQConsumer consumer,String topicName,int queueIdx,String consumerGroupName)throws Exception{
		check(listener,consumer,topicName);
		if(queueIdx<0) {
			throw new Exception("queue index is not right");
		}
		if(clientService==null) {
			clientService = consumer.clientService;
		}
		clientService.setMessageNotifyHandler(this);
		consumerMap.put(consumer.getSessionId(), consumer);
		
		
		MessageListenerContext ct = new MessageListenerContext(consumer, topicName,queueIdx);
		MessageListenerContext context = contextMap.get(ct.getId());
		if(context==null) {
			context = ct;
			contextMap.put(context.getId(), context);
		}
		ConsumerGroupContext groupContext = new ConsumerGroupContext(consumerGroupName, listener);
		context.putConsumerGroupContext(groupContext);
		registerQueue(context,consumerGroupName);
	}
	
	public void addListener(MessageListener listener,MQConsumer consumer,String topicName)throws Exception{
		String consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		addListener(listener, consumer, topicName, consumerGroupName);
	}
	
	public void addListener(MessageListener listener,MQConsumer consumer,String topicName,String consumerGroupName)throws Exception{
		check(listener,consumer,topicName);
		if(clientService==null) {
			clientService = consumer.clientService;
		}
		clientService.setMessageNotifyHandler(this);
		consumerMap.put(consumer.getSessionId(), consumer);
		MessageListenerContext ct =  new MessageListenerContext(consumer, topicName);
		MessageListenerContext context = contextMap.get(ct.getId());
		if(context==null) {
			context = ct;
			contextMap.put(context.getId(), context);
		}
		ConsumerGroupContext groupContext = new ConsumerGroupContext(consumerGroupName, listener);
		context.putConsumerGroupContext(groupContext);
		registerTopic(context,consumerGroupName);
	}
	
	private void check(MessageListener listener,MQConsumer consumer,String topicName)throws Exception{
		if(listener==null) {
			throw new Exception("messageListener is null");
		}
		if(consumer==null) {
			throw new Exception("Consumer is null");
		}
		if(topicName==null) {
			throw new Exception("topic name is null");
		}
	}
	
	
	public void removeListener(MQConsumer consumer,String topicName,int queueIdx){
		MessageListenerContext context = new MessageListenerContext(consumer, topicName,queueIdx);
		removeMessageListenContext(context.getId());
	}
	
	public void removeListener(MQConsumer consumer,String topicName){
		MessageListenerContext context = new MessageListenerContext(consumer, topicName);
		removeMessageListenContext(context.getId());
	}
	
	private void removeMessageListenContext(String id){
		MessageListenerContext context = contextMap.get(id);
		if(context==null){
			return;
		}
		consumerMap.remove(context.getConsumer().getSessionId());	
	}
	
	
	
	
	private void registerQueue(MessageListenerContext context,String congroupName)throws Exception{
		MQTopic topic = new MQTopic(MQDomain.DEFAULT_DOMAIN_ID, context.topicName);
		topic.buildTopicId();
		MQueue queue = new MQueue(topic, context.getQueueIdx());
		String queueId = queue.buildQueueId();
		MessageClient client = clientService.getMessageClientByQueueId(queueId);
		if(client==null){
			return ;
		}
		if(!client.isContected()){
			return ;
		}
		RegisterCommand command = new RegisterCommand(queue, TopicMapping.REG_TYPE_QUEUE);
		command.setSessionId(context.getConsumer().getSessionId());
		command.setCongroupName(congroupName);
		client.writeMessage(command);
		context.consumer.updateSession();
	}
	
	private void registerTopic(MessageListenerContext context,String congroupName)throws Exception{
		MQTopic t = new MQTopic(MQDomain.DEFAULT_DOMAIN_ID, context.getTopicName());
		String topicId = t.buildTopicId();
		MQTopic topic = clientService.getTopicInfo(MQDomain.DEFAULT_DOMAIN_ID, topicId);
		if(topic==null){
			return;
		}
		MQueue queue =  null;
		String queueId = null;
		RegisterCommand command = null;
		for(int i=0,n=topic.getQueueNum();i<n;i++){
			queue = new MQueue(topicId, i);
			if(queue.getId()==null) {
				queue.buildQueueId();	
			}
			queueId = queue.getId();
			MessageClient client = clientService.getMessageClientByQueueId(queueId);
			if(client==null){
				continue;
			}
			if(!client.isContected()){
				continue;
			}
			command = new RegisterCommand(queue, TopicMapping.REG_TYPE_TOPIC);
			command.setSessionId(context.getConsumer().getSessionId());
			command.setCongroupName(congroupName);
			client.writeMessage(command);
		}
		context.consumer.updateSession();
	}
	
	private void registerService(){
		if(running) {
			logger.info("running");
			return ;
		}
		try {
			logger.info("start to register service");
			running =true;
			for(MessageListenerContext context : contextMap.values()) {
				if(TopicMapping.REG_TYPE_QUEUE.equals(context.getRegisterType())) {
					registerQueue(context,null);
				}else if (TopicMapping.REG_TYPE_TOPIC.equals(context.getRegisterType())) {
					registerTopic(context,null);
				}
			}
		}catch(Exception e){
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
		}finally {
			running = false;
			logger.info("fininshed to register service");
		}
		
	}
	
	class MessageTask implements Runnable{
		private MessageListenService service ;
		private MessageNotifyContext context;
		
		public MessageTask(MessageListenService service,MessageNotifyContext context) {
			this.service = service;
			this.context = context;
		}

		@Override
		public void run() {

			service.doTask(context);
			
		}
		
	}
	
	class RegistedTask implements Runnable{
		private MessageListenService service;
		

		public RegistedTask(MessageListenService listenService) {
			this.service = listenService;
		}
		@Override
		public void run() {
			service.registerService();
		}
		
	}
	
	class ConsumerGroupContext{
		private String groupName;
		
		private MessageListener listener;
		
		public ConsumerGroupContext(String congroupName,MessageListener msgListener) {
			this.groupName = congroupName;
			this.listener = msgListener;
		}

		public String getGroupName() {
			return groupName;
		}

		public void setGroupName(String groupName) {
			this.groupName = groupName;
		}

		public MessageListener getListener() {
			return listener;
		}

		public void setListener(MessageListener listener) {
			this.listener = listener;
		}
		
	}
	
	class MessageListenerContext{
		private String id;
		
		private MQConsumer consumer;
		
		private String topicName;
		
		private int queue;
		
		private String registerType;
		
		private Map<String,ConsumerGroupContext> congroupMap;
		
		public MessageListenerContext(MQConsumer consumer,String topicName,int queueIdx){
			this.consumer = consumer;
			this.topicName = topicName;
			this.queue = queueIdx;
			this.registerType = TopicMapping.REG_TYPE_QUEUE;
			this.congroupMap = new HashMap<String,ConsumerGroupContext>();
			StringBuilder sb = new StringBuilder(100);
			sb.append(consumer.getSessionId());
			sb.append(":");
			sb.append(topicName);
			sb.append(":");
			sb.append(String.valueOf(queueIdx));
			this.id = sb.toString();
		}
		
		public MessageListenerContext(MQConsumer consumer,String topicName){
			this.consumer = consumer;
			this.topicName = topicName;
			this.registerType = TopicMapping.REG_TYPE_TOPIC;
			this.congroupMap = new HashMap<String,ConsumerGroupContext>();
			StringBuilder sb = new StringBuilder(100);
			sb.append(consumer.getSessionId());
			sb.append(":");
			sb.append(topicName);
			this.id = sb.toString();
		}
		
		public void putConsumerGroupContext(ConsumerGroupContext context) {
			congroupMap.put(context.getGroupName(), context);
		}

		public MQConsumer getConsumer() {
			return consumer;
		}

		public void setConsumer(MQConsumer consumer) {
			this.consumer = consumer;
		}

		public String getTopicName() {
			return topicName;
		}

		public void setTopicName(String topicName) {
			this.topicName = topicName;
		}

		public int getQueueIdx() {
			return queue;
		}

		public void setQueueIdx(int queueIdx) {
			this.queue = queueIdx;
		}

		public String getRegisterType() {
			return registerType;
		}

		public String getId() {
			return id;
		}

		public Map<String, ConsumerGroupContext> getCongroupMap() {
			return congroupMap;
		}

	}
	
}
