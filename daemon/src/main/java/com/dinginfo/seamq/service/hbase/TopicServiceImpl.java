package com.dinginfo.seamq.service.hbase;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.service.TopicService;
import com.dinginfo.seamq.storage.hbase.TopicHBaseStorage;

public class TopicServiceImpl implements TopicService {
	
	private TopicHBaseStorage topicStorage = null;
	
	private boolean loaded;

	
	public TopicServiceImpl(){
	}
	
	public void setTopicStorage(TopicHBaseStorage topicStorage) {
		this.topicStorage = topicStorage;
	}

	@Override
	public void createTopic(MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		topicStorage.createTopic(topic);
	}

	@Override
	public int updateTopic(MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		return topicStorage.updateTopic(topic);
	}

	@Override
	public int disableTopic(MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		return topicStorage.disableTopic(topic);
	}

	@Override
	public int enableTopic(MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		return topicStorage.enableTopic(topic);
	}

	@Override
	public int dropTopic(MQTopic topic) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		List<String> customerList = getConsumerGroupNameList(topic);
		return topicStorage.dropTopic(topic,customerList);
	}
	
	

	@Override
	public int disableQueue(MQueue queue) throws Exception {
		if(queue==null){
			throw new Exception("queue is null");
		}
		String queueId = queue.getId();
		if(queueId==null || queueId.trim().length()==0){
			queueId = queue.buildQueueId();
		}
		return topicStorage.disableQueue(queueId);
	}

	@Override
	public int enableQueue(MQueue queue) throws Exception {
		if(queue==null){
			throw new Exception("queue is null");
		}
		String queueId = queue.getId();
		if(queueId==null || queueId.trim().length()==0){
			queueId = queue.buildQueueId();
		}
		return topicStorage.enableQueue(queueId);
	}

	@Override
	public int increaseQueue(MQTopic topic, int queueNum) throws Exception {
		if(topic == null){
			throw new Exception("topic is null");
		}
		List<String> customerList = getConsumerGroupNameList(topic);
		return topicStorage.increaseQueue(topic, queueNum,customerList);
	}

	@Override
	public int reduceQueue(MQTopic topic, String queueId) throws Exception {
		if(topic==null){
			throw new Exception("topic is null");
		}
		MQueue queue = topicStorage.loadQueueByPK(queueId);
		if(queue.getInOffset()!=queue.getOutOffset()) {
			throw new Exception("queue is not empty");
		}
		List<String> customerList = getConsumerGroupNameList(topic);
		return topicStorage.reduceQueue(topic, queueId,customerList);
	}

	@Override
	public MQTopic getTopicByPK(String key) throws Exception {
		if(key==null){
			return null;
		}
		return topicStorage.getTopicByPK(key);
	}

	@Override
	public List<MQueue> getQueueList(MQTopic topic) throws Exception {
		if(topic==null) {
			return null;
		}
		int queueNum = topic.getQueueNum();
		if(queueNum<=0) {
			MQTopic t = topicStorage.getTopicByPK(topic.getId());
			if(t==null) {
				return null;
			}
			queueNum = t.getQueueNum();
		}
		List<MQueue> queueList = new ArrayList<MQueue>();
		MQueue queue = null;
		for(int i =0;i<queueNum;i++) {
			queue = new MQueue(topic.getId(), i);
			queue.buildQueueId();
			queue = topicStorage.loadQueueByPK(queue.getId());
			if(queue==null) {
				queue = createQueue(topic, i);
			}
			queueList.add(queue);
		}
		return queueList;
	}
	
	private MQueue createQueue(MQTopic topic,int queue)throws Exception{
		long offset =0;
		Date date = Calendar.getInstance().getTime();
		MQueue q = new MQueue(topic, queue);
		q.buildQueueId();
		q.setDomain(topic.getDomain());
		q.setInOffset(offset);
		q.setCreatedTime(date);
		topicStorage.putQueue(q);
		OutPosition outp = topicStorage.getOutPositionByPK(q.getId());
		if(outp==null) {
			outp = new OutPosition();
			outp.setId(q.getId());
			outp.setOutOffset(offset);
			outp.setQueueId(q.getId());
			topicStorage.createOutPosition(outp);	
		}
		return q;
	}

	@Override
	public int grantUserToProducer(MQTopic topic) throws Exception {
		if(topic==null || topic.getUser()==null){
			throw new Exception("topic is null");
		}
		return topicStorage.grantUserToProducer(topic);
	}

	@Override
	public MQueue getQueueByPK(MQueue queue) throws Exception {
		if(queue==null){
			return null;
		}
		String queueId = queue.getId();
		if(queueId==null || queueId.trim().length()==0){
			queueId = queue.buildQueueId();
		}
		return topicStorage.loadQueueByPK(queueId);
	}

	@Override
	public int updateQueueInOffset(MQueue queue) throws Exception {
		return topicStorage.updateQueueInOffset(queue);
	}

	@Override
	public int updateQueueOutOffset(MQueue queue) throws Exception {
		return topicStorage.updateQueueOutOffset(queue);
	}

	@Override
	public int getTopicCount() throws Exception {
		Long n = topicStorage.getTopicCount(MQDomain.DEFAULT_DOMAIN_ID); 
		if(n==null){
			return 0;
		}else{
			return n.intValue();	
		}
	}

	@Override
	public List<MQTopic> getTopicList(MQTopic topic,int pageSize,int pageNo) throws Exception {
		return topicStorage.getTopicList(topic.getDomain().getId(), pageSize,pageNo);
	}

	@Override
	public List<User> getUserListByTopic(String topicId) throws Exception {
		return topicStorage.getUserListByTopic(topicId);
	}
	
	
	@Override
	public List<OutPosition> getOutPositionList(String queueId) throws Exception {
		List<OutPosition> outList = new ArrayList<OutPosition>();
		MQueue queue = topicStorage.getQueueByPK(queueId);
		if(queue==null) {
			return outList;
		}
		MQTopic topic = topicStorage.getTopicByPK(queue.getTopic().getId());
		List<String> gnameList = getConsumerGroupNameList(topic);
		if(gnameList==null) {
			return outList;
		}
		OutPosition bean = new OutPosition();
		OutPosition out = null;
		for(String gname : gnameList) {
			out = topicStorage.getOutPositionByPK(bean.buildId(queueId, gname));
			if(out==null) {
				out = new OutPosition();
				out.setId(bean.buildId(queueId, gname));
				out.setQueueId(queueId);
				out.setOutOffset(0);
				out.setGroupName(gname);
				topicStorage.createOutPosition(out);
			}
			outList.add(out);
		}
		return outList;
	}

	@Override
	public void createConsumerGroup(ConsumerGroup group) throws Exception {
		topicStorage.createConsumerGroup(group);
	}

	@Override
	public int dropConsumerGroupByPK(ConsumerGroup group) throws Exception {
		return topicStorage.dropConsumerGroupByPK(group);
	}

	@Override
	public ConsumerGroup getConsumerGroupByPK(ConsumerGroup group) throws Exception {
		return topicStorage.getConsumerGroupByPK(group);
	}

	@Override
	public List<ConsumerGroup> getConsumerGroupList(String topicId) throws Exception {
		List<ConsumerGroup> groupList = topicStorage.getConsumerGroupList(topicId);
		if(groupList==null || groupList.size()==0) {
			ConsumerGroup group = new ConsumerGroup();
			group.setName(ConsumerGroup.DEFAULT_GROUP_NAME);
			MQTopic topic = topicStorage.getTopicByPK(topicId);
			if(topic==null) {
				return null;
			}
			group.setTopic(topic);
			topicStorage.createConsumerGroup(group);
			groupList = topicStorage.getConsumerGroupList(topicId);
		}
		return groupList;
	}

	@Override
	public int removeProducerUser(MQTopic topic) throws Exception {
		return topicStorage.removeProducerUser(topic);
	}

	@Override
	public List<User> getProducerUserList(String topicId) throws Exception {
		return topicStorage.getProducerUserList(topicId);
	}

	@Override
	public User getProducerUserByPK(MQTopic topic) throws Exception {
		return topicStorage.getProducerUserByPK(topic);
	}

	@Override
	public int grantUserToConsumerGroup(ConsumerGroup group) throws Exception {
		return topicStorage.grantUserToConsumerGroup(group);
	}

	@Override
	public int removeConsumerUser(ConsumerGroup group) throws Exception {
		return topicStorage.removeConsumerUser(group);
	}

	@Override
	public User getConsumerUserByPK(ConsumerGroup group) throws Exception {
		return topicStorage.getConsumerUserByPK(group);
	}

	@Override
	public List<User> getConsumerUserList(String topicId) throws Exception {
		return topicStorage.getConsumerUserList(topicId);
	}

	@Override
	public synchronized boolean load() throws Exception {
		loaded = true;
		return loaded;
	}

	@Override
	public void clear() {
		loaded = false;
	}

	@Override
	public boolean isLoaded() {
		return loaded;
	}
	
	private List<String> getConsumerGroupNameList(MQTopic topic)throws Exception{
		if(topic==null || topic.getId()==null) {
			return null;
		}
		
		List<ConsumerGroup> groupList = topicStorage.getConsumerGroupList(topic.getId());
		if(groupList==null) {
			return null;
		}
		List<String> customerList = new ArrayList<String>();
		for(ConsumerGroup group : groupList) {
			customerList.add(group.getName());
		}
		return customerList;
	}
}
