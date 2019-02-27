package com.dinginfo.seamq.commandhandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.MQueueAgent;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;
import com.dinginfo.seamq.ResponseStatus;
import com.dinginfo.seamq.ServiceContext;
import com.dinginfo.seamq.SocketContext;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.entity.mapping.MessageMapping;
import com.dinginfo.seamq.entity.mapping.TopicMapping;
import com.dinginfo.seamq.service.NotifyService;
import com.dinginfo.seamq.service.QueueService;

import io.netty.channel.Channel;

public class RegisterHandler implements CommandHandler, ResponseStatus {
	private final static Logger logger = LogManager.getLogger(MessageHandler.class);
	
	private NotifyService notifyService;
	
	private QueueService queueService;

	
	public RegisterHandler(){
		queueService = MyBean.getBean(QueueService.BEAN_NAME, QueueService.class);
		
	}
	@Override
	public void doCommand(MessageRequest request, MessageResponse response) {
		if(!request.getServerContext().isNotifyMessage()){
			response.writeMessage(STATUS_ERROR, "No need notify Service");
			return;
		}
		
		if(notifyService==null) {
			ServiceContext sc = request.getServerContext();
			notifyService = sc.getBean(NotifyService.BEAN_NAME, NotifyService.class);
		}
		if(notifyService==null) {
			response.writeMessage(STATUS_ERROR, "No notifyService");
			return;
		}
		
		User user = request.getUser();
		SocketContext context = new SocketContext();
		context.setDomainId(user.getDomain().getId());
		context.setUser(user.getName());
		Channel channel = request.getChannel();
		context.setChannel(channel);
		context.setSessionId(request.getSessionId());
		String type = request.getStringField(TopicMapping.REG_TYPE);
		String queueId = request.getStringField(MessageMapping.FIELD_QUEUE_ID);
		MQueue queue = new MQueue(queueId);
		String consumerGroupName = request.getStringField(MessageMapping.FIELD_CONSUMER_GROUP);
		if(consumerGroupName==null || consumerGroupName.trim().length()==0) {
			consumerGroupName = ConsumerGroup.DEFAULT_GROUP_NAME;
		}
		
		try {
			MQueueAgent queueAgent = queueService.getQueueAgent(queue);
			ConsumerGroup cgroup = new ConsumerGroup();
			cgroup.setTopic(new MQTopic(queueAgent.getTopic().getId()));
			cgroup.setName(consumerGroupName);
			String topicId = queueAgent.getTopic().getId();
			if(!queueService.checkOutPermission(user, cgroup)) {
				response.writeMessage(STATUS_ERROR, "not out permission for this topic");
				return;
			}	
			
			if(TopicMapping.REG_TYPE_QUEUE.equals(type)){
				notifyService.registerQueue(context, queue);	
			}else if(TopicMapping.REG_TYPE_TOPIC.equals(type)){
				
				MQTopic topic = queueAgent.getTopic();
				MQTopic t = new MQTopic(topic.getId());
				notifyService.registerTopic(context, t);
			}
			response.setStatus(ResponseStatus.STATUS_SUCCESS);
			response.writeMessage();	
			sendNotify(request, queueAgent);
		}catch(Exception e) {
			response.writeMessage(STATUS_ERROR, e.getMessage());
			StringWriter writer = new StringWriter();
			PrintWriter pw = new PrintWriter(writer);
			e.printStackTrace(pw);
			logger.error(writer.toString());
			response.writeMessage(STATUS_ERROR, e.getMessage());
		}
	}
	
	private void sendNotify(MessageRequest request,MQueueAgent queueAgent) throws Exception{
		ServiceContext context = request.getServerContext();
		if(!context.isNotifyMessage()) {
			return;
		}
		long notifyOffset = 0;
		String topicId = queueAgent.getTopic().getId();
		List<String> consumerGroupNameList = queueService.getConsumerGroupNameList(topicId);
		if(consumerGroupNameList==null) {
			return ;
		}
		
		for(String groupName : consumerGroupNameList) {
			notifyOffset = queueAgent.getNotifyOffset(groupName);
			if(notifyOffset>0) {
				MQueue notifyQueue = new MQueue();
				notifyQueue.setQueue(queueAgent.getQueue().getQueue());
				notifyQueue.setId(queueAgent.getQueue().getId());
				MQTopic notifyTopic = new MQTopic();
				notifyTopic.setName(queueAgent.getTopic().getName());
				notifyTopic.setId(queueAgent.getTopic().getId());
				notifyQueue.setTopic(notifyTopic);
				MQDomain notifyDomain = new MQDomain();
				notifyDomain.setId(queueAgent.getDomain().getId());
				notifyQueue.setDomain(notifyDomain);
				notifyService.sendMessage(notifyQueue);
				break;
			}
		}
	}
}
