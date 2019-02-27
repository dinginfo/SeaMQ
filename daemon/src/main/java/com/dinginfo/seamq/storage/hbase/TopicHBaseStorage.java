/*
 * Copyright 2016 David Ding.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.dinginfo.seamq.storage.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dinginfo.seamq.MQConstant;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.storage.hbase.mapping.ConsumerGroupTable;
import com.dinginfo.seamq.storage.hbase.mapping.OutPositionTable;
import com.dinginfo.seamq.storage.hbase.mapping.QueueTable;
import com.dinginfo.seamq.storage.hbase.mapping.TopicTable;
import com.dinginfo.seamq.storage.hbase.mapping.TopicUserTable;

public class TopicHBaseStorage extends BasicHBaseStorage{
	public static final String BEAN_NAME = "topicStorage";
	
	private final byte[] CF = Bytes.toBytes("cf");

	private HBaseDataSource dataSource;
	
	private TableName topicTableName = null;
	
	private TableName queueTableName = null;
	
	private TableName outPositionTableName = null;
	
	private TableName topicUserTableName = null;
	
	private TableName consumerGroupTableName = null;
	
	public TopicHBaseStorage(String namespace){
		
		topicTableName =TableName.valueOf(HBaseUtils.buildTalbeName(namespace, TopicTable.TABLE_NAME));
		queueTableName =TableName.valueOf(HBaseUtils.buildTalbeName(namespace, QueueTable.TABLE_NAME));
		outPositionTableName=TableName.valueOf(HBaseUtils.buildTalbeName(namespace, OutPositionTable.TABLE_NAME));
		topicUserTableName =TableName.valueOf(HBaseUtils.buildTalbeName(namespace, TopicUserTable.TABLE_NAME));
		consumerGroupTableName = TableName.valueOf(HBaseUtils.buildTalbeName(namespace, ConsumerGroupTable.TABLE_NAME));
	}

	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void createTopic(MQTopic topic) throws Exception {
		Connection conn = dataSource.getConnection();
		Table tb = conn.getTable(topicTableName);
		try {
			byte[] rowKey = Bytes.toBytes(topic.getId());
			Date date = Calendar.getInstance().getTime();
			if(topic.getCreatedTime()!=null) {
				date = topic.getCreatedTime();
			}
			Put put = new Put(rowKey);
			put.addColumn(CF, TopicTable.FIELD_TOPIC_ID, Bytes.toBytes(topic.getId()));
			put.addColumn(CF, TopicTable.FIELD_NAME, Bytes.toBytes(topic.getName()));
			put.addColumn(CF, TopicTable.FIELD_QUEUE_NUM, Bytes.toBytes(topic.getQueueNum()));
			put.addColumn(CF, TopicTable.FIELD_DOMAIN, Bytes.toBytes(topic.getDomain().getId()));
			put.addColumn(CF, TopicTable.FIELD_CREATED_TIME, Bytes.toBytes(date.getTime()));
			put.addColumn(CF, TopicTable.FIELD_UPDATE_TIME, Bytes.toBytes(date.getTime()));
			if (topic.getRemark() != null) {
				put.addColumn(CF, TopicTable.FIELD_REMARK, Bytes.toBytes(topic.getRemark()));
			}
			boolean ok= false;
			if(hbaseVersion==1) {
				ok = tb.checkAndPut(rowKey, CF, TopicTable.FIELD_TOPIC_ID, null, put);
			}else {
				ok = tb.checkAndMutate(rowKey, CF).ifNotExists().thenPut(put);
			}
			
			if(!ok){
				return ;
			}
			
			//for consumer group
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(topic);
			group.setName(ConsumerGroup.DEFAULT_GROUP_NAME);
			createConsumerGroup(group);
			//
			
			MQueue queue = null;
			OutPosition position = null;
			long offset =0;			
			for(int i=0,n=topic.getQueueNum();i<n;i++){
				queue = new MQueue(topic.getId(),i);
				queue.buildQueueId();
				queue.setDomain(topic.getDomain());
				queue.setCreatedTime(date);
				queue.setInOffset(offset);
				putQueue(queue);
				// for out position info
				position = new OutPosition();
				position.setId(queue.getId());
				position.setOutOffset(offset);
				position.setQueueId(queue.getId());
				createOutPosition(position);
				
				position = new OutPosition();
				position.setId(position.buildId(queue.getId(), ConsumerGroup.DEFAULT_GROUP_NAME));
				position.setOutOffset(0L);
				position.setQueueId(queue.getId());
				position.setGroupName(ConsumerGroup.DEFAULT_GROUP_NAME);
				position.setTopicId(topic.getId());
				createOutPosition(position);
			}
		}finally{
			HBaseUtils.closeTable(tb);
		}

	}
	
	public boolean putQueue(MQueue queue)throws Exception{
		Table tb = null;
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);	
			Put pt = new Put(Bytes.toBytes(queue.getId()));
			pt.addColumn(CF, QueueTable.FIELD_QUEUE_ID,Bytes.toBytes(queue.getId()));
			pt.addColumn(CF, QueueTable.FIELD_DOMAIN_ID, Bytes.toBytes(queue.getDomain().getId()));
			pt.addColumn(CF, QueueTable.FIELD_QUEUE, Bytes.toBytes(queue.getQueue()));
			pt.addColumn(CF, QueueTable.FIELD_TOPIC_ID, Bytes.toBytes(queue.getTopic().getId()));
			pt.addColumn(CF, QueueTable.FIELD_CREATED_TIME, Bytes.toBytes(queue.getCreatedTime().getTime()));
			pt.addColumn(CF, QueueTable.FIELD_IN_OFFSET, Bytes.toBytes(queue.getInOffset()));
			tb.put(pt);
			return true;
		}finally {
			HBaseUtils.closeTable(tb);
		}
	}
	
	
	public boolean createOutPosition(OutPosition out) throws Exception{
		Connection conn = null;
		Table tb = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(outPositionTableName);
			byte[] rowKey = Bytes.toBytes(out.getId());
			Put pt = new Put(rowKey);
			pt.addColumn(CF, OutPositionTable.FIELD_OUT_OFFSET, Bytes.toBytes(out.getOutOffset()));
			pt.addColumn(CF, OutPositionTable.FIELD_QUEUE_ID, Bytes.toBytes(out.getQueueId()));
			if(out.getGroupName()!=null && out.getGroupName().trim().length()>0) {
				pt.addColumn(CF, OutPositionTable.FIELD_GROUP_NAME, Bytes.toBytes(out.getGroupName()));	
			}
			if(hbaseVersion==1) {
				return tb.checkAndPut(rowKey, CF, OutPositionTable.FIELD_QUEUE_ID, null, pt);
			}else {
				return tb.checkAndMutate(rowKey, CF).ifNotExists().thenPut(pt);
			}
		}finally {
			HBaseUtils.closeTable(tb);
		}
	}
	
	public long getTopicCount(long domainId) throws Exception {
		Connection conn = null;
		Table tb = null;
		ResultScanner rs = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicTableName);
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			rs = tb.getScanner(scan);
			long counter =0;
			for(Result r : rs){
				counter = counter + r.size();
			}
			return counter;
		}finally{
			HBaseUtils.closeResult(rs);
			HBaseUtils.closeTable(tb);
		}
	}

	
	public List<MQTopic> getTopicList(long domainId, int pageSize,int pageNo) throws Exception {
		Scan scan = new Scan();
		List<MQTopic> topicList =new ArrayList<MQTopic>();
		long startIndex = pageSize * pageNo;
		long endIndex = startIndex+pageSize;
		long index=0;
		MQTopic topic = null;
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicTableName);
			
			ResultScanner rs = tb.getScanner(scan);
			Result rt =null;
			byte[] value =null;
			
			while(true){
				rt = rs.next();
				if(rt==null){
					break;
				}
				if(index<startIndex){
					continue;
				}
				
				if(index>=endIndex){
					break;
				}
				value = rt.getValue(CF, TopicTable.FIELD_NAME);
				if(value==null){
					continue;
				}
				
				topic = new MQTopic();
				topic.setName(Bytes.toString(value));
				value = rt.getValue(CF, TopicTable.FIELD_DOMAIN);
				if(value!=null){
					topic.setDomain(new MQDomain(Bytes.toLong(value)));
				}
				if(domainId!=topic.getDomain().getId()){
					continue;
				}
				
				topic.setId(Bytes.toString(rt.getRow()));
				
				value = rt.getValue(CF, TopicTable.FIELD_QUEUE_NUM);
				if(value!=null){
					topic.setQueueNum(Bytes.toInt(value));
				}
				value = rt.getValue(CF, TopicTable.FIELD_TYPE);
				value = rt.getValue(CF, TopicTable.FIELD_REMARK);
				topic.setRemark(Bytes.toString(value));
				value = rt.getValue(CF, TopicTable.FIELD_STATUS);
				topic.setStatus(Bytes.toString(value));
				value = rt.getValue(CF, TopicTable.FIELD_CREATED_TIME);
				if(value!=null){
					Date createdTime = new Date(Bytes.toLong(value));
					topic.setCreatedTime(createdTime);
				}
				topicList.add(topic);
				index++;
			}
			return topicList;
		}finally{
			HBaseUtils.closeTable(tb);	
		}
	}

	
	public int disableTopic(MQTopic topic) throws Exception {
		MQTopic t = getTopicByPK(topic.getId());
		if(t==null){
			return 0;
		}
		int qnum = t.getQueueNum();
		
		MQueue queue = null;
		Put ptQueue  = null;
		List<Put> ptList = new ArrayList<Put>();
		for(int i=0;i<qnum;i++){
			queue = new MQueue(topic.getId(),i);
			ptQueue = new Put(Bytes.toBytes(queue.buildQueueId()));
			ptQueue.addColumn(CF, QueueTable.FIELD_STATUS, Bytes.toBytes(MQTopic.STATUS_DISABLE));
			ptList.add(ptQueue);
		}
		Put ptTopic = new Put(Bytes.toBytes(topic.getId()));
		ptTopic.addColumn(CF, TopicTable.FIELD_STATUS, Bytes.toBytes(MQTopic.STATUS_DISABLE));
		Connection conn = dataSource.getConnection();
		Table tbTopic = null;
		Table tbQueue = null;
		try{
			
			tbTopic = conn.getTable(topicTableName);
			tbQueue = conn.getTable(queueTableName);
			tbQueue.put(ptList);
			tbTopic.put(ptTopic);
			return 1;
		}finally{
			HBaseUtils.closeTable(tbQueue);
			HBaseUtils.closeTable(tbTopic);
		}
	}

	
	public int enableTopic(MQTopic topic) throws Exception {
		if(topic==null){
			return 0;
		}
		int qnum = topic.getQueueNum();
		MQueue queue = null;
		Delete delQueue = null;
		List<Delete> delList = new ArrayList<Delete>();
		for(int i=0;i<qnum;i++){
			queue = new MQueue(topic.getId(),i);
			delQueue = new Delete(Bytes.toBytes(queue.buildQueueId()));
			delQueue.addColumn(CF, QueueTable.FIELD_STATUS);
			delList.add(delQueue);
		}
		Delete delTopic = new Delete(Bytes.toBytes(topic.getId()));
		delTopic.addColumn(CF, TopicTable.FIELD_STATUS);
		Connection conn = null;
		Table tbTopic = null;
		Table tbQueue = null;
		try{
			conn = dataSource.getConnection();
			tbTopic = conn.getTable(topicTableName);
			tbQueue = conn.getTable(queueTableName);
			tbQueue.delete(delList);
			tbTopic.delete(delTopic);
			return 1;
		}finally{
			HBaseUtils.closeTable(tbTopic);
			HBaseUtils.closeTable(tbQueue);
		}
	}

	
	public int dropTopic(MQTopic topic,List<String> outPositionList) throws Exception {
		MQTopic t = getTopicByPK(topic.getId());
		if (t == null) {
			return 0;
		}
		int qnum = t.getQueueNum();
		Connection conn = null;
		Table tbTopic = null;
		Table tbTopicUser = null;
		try{
			
			//check queue
			MQueue queue = null;
			String queueId = null;
			for (int i = 0; i < qnum; i++) {
				queue = new MQueue(t.getId(),i);
				queueId = queue.buildQueueId();
				queue = loadQueueByPK(queueId);
				if(queue==null){
					continue;
				}
				if(queue.getOutOffset()<queue.getInOffset()){
					StringBuilder sb = new StringBuilder(200);
					sb.append("topic:");
					sb.append(t.getName());
					sb.append(",queue:");
					sb.append(queue.getQueue());
					sb.append(",has message");
					throw new Exception(sb.toString());
				}
				
			}
			
			OutPosition outp = new OutPosition();
			for (int i = 0; i < qnum; i++) {
				queue = new MQueue(topic.getId(), i);
				queue.buildQueueId();
				deleteOutPositionByPK(queue.getId());
				deleteQueueByPK(queue.getId());
				if(outPositionList!=null) {
					for(String userId : outPositionList) {
						deleteOutPositionByPK(outp.buildId(queue.getId(), userId));
					}
				}
			}
			conn = dataSource.getConnection();

			tbTopic = conn.getTable(topicTableName);
			tbTopicUser = conn.getTable(topicUserTableName);
			//delete user of topic info
			byte[] rowKey = Bytes.toBytes(topic.getId());
			Delete del = new Delete(rowKey);
			tbTopicUser.delete(del);
			
			//delete topic info
			del = new Delete(rowKey);
			tbTopic.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tbTopic);
			HBaseUtils.closeTable(tbTopicUser);
		}
	}

	
	public int disableQueue(String queueId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);
			Put pt = new Put(Bytes.toBytes(queueId));
			pt.addColumn(CF, QueueTable.FIELD_STATUS, Bytes.toBytes(MQTopic.STATUS_DISABLE));
			tb.put(pt);	
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int enableQueue(String queueId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);
			Delete del = new Delete(Bytes.toBytes(queueId));
			del.addColumn(CF, QueueTable.FIELD_STATUS);
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int increaseQueue(MQTopic topic, int queueNum,List<String> outPositionList) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicTableName);
			int num = topic.getQueueNum();
			Put pt = new Put(Bytes.toBytes(topic.getId()));
			pt.addColumn(CF, TopicTable.FIELD_QUEUE_NUM, Bytes.toBytes(num+queueNum));
			tb.put(pt);
			MQueue queue = null;
			long offset =0;
			Date date = Calendar.getInstance().getTime();
			OutPosition outp = null;
			for(int i=num,n=num+queueNum;i<n;i++){
				queue = new MQueue(topic.getId(),i);
				queue.buildQueueId();
				//for queue info
				queue.setDomain(topic.getDomain());
				queue.setTopic(topic);
				queue.setCreatedTime(date);
				queue.setInOffset(offset);
				putQueue(queue);
				
				// for out info
				outp = new OutPosition();
				outp.setId(queue.getId());
				outp.setOutOffset(offset);
				outp.setQueueId(queue.getId());
				createOutPosition(outp);
				
				//for out user
				if(outPositionList!=null) {
					for(String groupName : outPositionList) {
						outp = new OutPosition();
						outp.setId(outp.buildId(queue.getId(), groupName));
						outp.setOutOffset(offset);
						outp.setQueueId(queue.getId());
						outp.setGroupName(groupName);
						createOutPosition(outp);
					}
				}
			}
			return queueNum;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int reduceQueue(MQTopic topic, String queueId,List<String> outPositionList) throws Exception {
		if(topic==null){
			return 0;
		}
		int queueNum = topic.getQueueNum()-1;
		if(queueNum<1){
			return 0;
		}
		Connection conn = null;
		Table tb = null;
		try{
			MQueue queue = loadQueueByPK(queueId);
			if(queue!=null && (queue.getOutOffset()<queue.getInOffset())){
				StringBuilder sb = new StringBuilder(200);
				sb.append("topic:");
				sb.append(topic.getName());
				sb.append(",queue:");
				sb.append(queue.getQueue());
				sb.append(",has message");
				throw new Exception(sb.toString());
			}
			OutPosition outp = new OutPosition();
			if(outPositionList!=null) {
				for(String groupName : outPositionList) {
					deleteOutPositionByPK(outp.buildId(queue.getId(), groupName));
				}
			}
			deleteOutPositionByPK(queue.getId());
			deleteQueueByPK(queue.getId());
			
			//update topic info
			conn = dataSource.getConnection();
			Put pt= new Put(Bytes.toBytes(topic.getId()));
			pt.addColumn(CF, TopicTable.FIELD_QUEUE_NUM, Bytes.toBytes(queueNum));
			tb = conn.getTable(topicTableName);
			tb.put(pt);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public MQTopic getTopicByPK(String key) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicTableName);
			Get get = new Get(Bytes.toBytes(key));
			Result rt = tb.get(get);
			
			if(rt==null || rt.size()==0){
				return null;
			}
			MQTopic topic = new MQTopic();
			topic.setId(Bytes.toString(rt.getValue(CF, TopicTable.FIELD_TOPIC_ID)));
			topic.setName(Bytes.toString(rt.getValue(CF, TopicTable.FIELD_NAME)));
			topic.setQueueNum(Bytes.toInt(rt.getValue(CF, TopicTable.FIELD_QUEUE_NUM)));
			topic.setDomain(new MQDomain(Bytes.toLong(rt.getValue(CF, TopicTable.FIELD_DOMAIN))));
			byte[] data = rt.getValue(CF, TopicTable.FIELD_REMARK);
			if (data != null) {
				topic.setRemark(Bytes.toString(data));
			}
			data = rt.getValue(CF, TopicTable.FIELD_STATUS);
			if(data!=null){
				topic.setStatus(Bytes.toString(data));
			}
			data = rt.getValue(CF, TopicTable.FIELD_CREATED_TIME);
			if (data != null) {
				topic.setCreatedTime(new Date(Bytes.toLong(data)));
			}
			data = rt.getValue(CF, TopicTable.FIELD_UPDATE_TIME);
			if(data!=null){
				topic.setUpdatedTime(new Date(Bytes.toLong(data)));
			}
			return topic;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}
	
	private String buildTopicUserOID(String topicId,String username)throws Exception{
		if(topicId==null || topicId.trim().length()<=8) {
			throw new Exception("topic is null");
		}
		if(username==null || username.trim().length()==0) {
			throw new Exception("username is null");
		}
		StringBuilder sb = new StringBuilder(64);
		sb.append(topicId);
		sb.append(MQConstant.SEPARATOR);
		sb.append(username);
		String id = StringUtil.generateMD5String(sb.toString());
		sb = new StringBuilder(64);
		sb.append(topicId.substring(0, 8));
		sb.append(MQConstant.SEPARATOR);
		sb.append(id);
		return sb.toString();
	}

	
	public MQueue loadQueueByPK(String queueId) throws Exception {
		Connection conn = null;
		Table tbOut = null;
		try{
			
			MQueue queue = getQueueByPK(queueId);
			if(queue==null) {
				return null;
			}
			conn = dataSource.getConnection();
			tbOut = conn.getTable(outPositionTableName);
			Get get = new Get(Bytes.toBytes(queueId));
			Result rt = tbOut.get(get);
			if(rt!=null && rt.size()>0){
				queue.setOutOffset(Bytes.toLong(rt.getValue(CF, OutPositionTable.FIELD_OUT_OFFSET)));
			}
			return queue;
		}finally{
			HBaseUtils.closeTable(tbOut);
		}
		
	}
	
	public MQueue getQueueByPK(String queueId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);
			MQueue q = new MQueue(queueId);
			Get get = new Get(Bytes.toBytes(q.getId()));
			Result rt = tb.get(get);
			if (rt == null || rt.size()==0) {
				return null;
			}
			
			MQueue queue = new MQueue(queueId);
			MQTopic topic = new MQTopic();
			topic.setId(Bytes.toString(rt.getValue(CF, QueueTable.FIELD_TOPIC_ID)));
			queue.setTopic(topic);
			queue.setDomain(new MQDomain(Bytes.toLong(rt.getValue(CF, QueueTable.FIELD_DOMAIN_ID))));
			queue.setQueue(Bytes.toInt(rt.getValue(CF, QueueTable.FIELD_QUEUE)));
			queue.setStatus(Bytes.toString(rt.getValue(CF, QueueTable.FIELD_STATUS)));
			queue.setInOffset(Bytes.toLong(rt.getValue(CF, QueueTable.FIELD_IN_OFFSET)));
			byte[] data = rt.getValue(CF, QueueTable.FIELD_CREATED_TIME);
			if(data!=null && data.length>0) {
				queue.setCreatedTime(new Date(Bytes.toLong(data)));
			}
			return queue;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int updateQueueInOffset(MQueue queue) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);
			Put put = new Put(Bytes.toBytes(queue.getId()));
			put.addColumn(CF, QueueTable.FIELD_IN_OFFSET, Bytes.toBytes(queue.getInOffset()));
			tb.put(put);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int updateQueueOutOffset(MQueue queue) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(outPositionTableName);
			byte[] rowKey = Bytes.toBytes(queue.getId());
			Put put = new Put(rowKey);
			put.addColumn(CF, OutPositionTable.FIELD_OUT_OFFSET, Bytes.toBytes(queue.getOutOffset()));
			tb.put(put);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}
	
	public int updateTopic(MQTopic topic) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicTableName);
			String topicId =topic.getId();
			if(topicId==null || topicId.trim().length()==0){
				topicId = topic.buildTopicId();
			}
			String remark = topic.getRemark();
			if(remark!=null && remark.trim().length()>0){
				Put put = new Put(Bytes.toBytes(topicId));
				put.addColumn(CF, TopicTable.FIELD_REMARK, Bytes.toBytes(remark));
				tb.put(put);
			}else{
				Delete del = new Delete(Bytes.toBytes(topicId));
				del.addColumn(CF, TopicTable.FIELD_REMARK);
				tb.delete(del);
			}
			
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}


	
	public String getGrantType(MQTopic topic) throws Exception {
		if(topic.getUser()==null || topic.getUser().getDomain()==null){
			return null;
		}

		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String username = topic.getUser().getName();
			String rowKey = buildTopicUserOID(topic.getId(), username);
			
			Get get = new Get(Bytes.toBytes(rowKey));
			Result rt = tb.get(get);
			if (rt == null || rt.size()==0) {
				return null;
			} 
			return Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_TYPE));	
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public List<User> getUserListByTopic(String topicId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			
			String prefix = topicId.substring(0, 8);
			Scan scan = new Scan();
			scan.setRowPrefixFilter(Bytes.toBytes(prefix));
			ResultScanner rs = tb.getScanner(scan);
			if(rs==null) {
				return null;
			}
			List<User> userList = new ArrayList<User>();
			Result r = null;
			User user = null;
			String tid = null;
			while(true) {
				r = rs.next();
				if(r==null) {
					break;
				}
				
				tid = Bytes.toString(r.getValue(CF, TopicUserTable.FIELD_TOPIC_ID));
				if(!topicId.equals(tid)) {
					continue;
				}
				user = new User();
				user.setName(Bytes.toString(r.getValue(CF, TopicUserTable.FIELD_USER)));
				userList.add(user);
			}
			return userList;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}
	
	public boolean deleteQueueByPK(String key)throws Exception{
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(queueTableName);
			Delete del = new Delete(Bytes.toBytes(key));
			tb.delete(del);
			return true;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	public boolean deleteOutPositionByPK(String key)throws Exception{
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(outPositionTableName);
			Delete del = new Delete(Bytes.toBytes(key));
			tb.delete(del);
			return true;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	public OutPosition getOutPositionByPK(String key)throws Exception{
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(outPositionTableName);
			Get get = new Get(Bytes.toBytes(key));
			Result rt = tb.get(get);
			if (rt == null || rt.size()==0) {
				return null;
			}
			
			OutPosition outp = new OutPosition();
			outp.setId(key);
			outp.setOutOffset(Bytes.toLong(rt.getValue(CF, OutPositionTable.FIELD_OUT_OFFSET)));
			outp.setQueueId(Bytes.toString(rt.getValue(CF, OutPositionTable.FIELD_QUEUE_ID)));
			outp.setGroupName(Bytes.toString(rt.getValue(CF, OutPositionTable.FIELD_GROUP_NAME)));
			return outp;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	public int increaseOutPosition(OutPosition out)throws Exception{
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(outPositionTableName);
			byte[] rowKey =Bytes.toBytes(out.getId());
			Put pt = new Put(rowKey);
			pt.addColumn(CF, OutPositionTable.FIELD_OUT_OFFSET, Bytes.toBytes(out.getOutOffset()+1));
			boolean successOffset = false;
			if(hbaseVersion ==1) {
				successOffset=tb.checkAndPut(rowKey, CF, OutPositionTable.FIELD_OUT_OFFSET, Bytes.toBytes(out.getOutOffset()), pt);	
			}else {
				successOffset = tb.checkAndMutate(rowKey, CF).qualifier(OutPositionTable.FIELD_OUT_OFFSET).ifEquals(Bytes.toBytes(out.getOutOffset())).thenPut(pt);
			}
			if(successOffset){
				return 1;
			}else{
				return 0;
			}
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	
	public void createConsumerGroup(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(consumerGroupTableName);
			byte[] rowKey =Bytes.toBytes(buildConsumerGroupRowKey(group));
			Put pt = new Put(rowKey);
			pt.addColumn(CF, ConsumerGroupTable.FIELD_NAME,Bytes.toBytes(group.getName()));
			pt.addColumn(CF, ConsumerGroupTable.FIELD_TOPIC_ID, Bytes.toBytes(group.getTopic().getId()));
			pt.addColumn(CF, ConsumerGroupTable.FIELD_DOMAIN_ID, Bytes.toBytes(group.getTopic().getDomain().getId()));
			boolean success = false;
			if(hbaseVersion ==1) {
				success=tb.checkAndPut(rowKey, CF, ConsumerGroupTable.FIELD_NAME, null, pt);	
			}else {
				success = tb.checkAndMutate(rowKey, CF).ifNotExists().thenPut(pt);
			}
			if(!success) {
				return ;
			}
			MQTopic topic = getTopicByPK(group.getTopic().getId());
			int queueNum = topic.getQueueNum();
			MQueue queue = null;
			OutPosition position = null;
			for(int i=0;i<queueNum;i++) {
				queue = new MQueue(topic.getId(), i);
				queue.buildQueueId();
				position = new OutPosition();
				position.setTopicId(topic.getId());
				position.setGroupName(group.getName());
				position.setQueueId(queue.getId());
				position.setOutOffset(0);
				position.setId(position.buildId(queue.getId(), group.getName()));
				createOutPosition(position);
			}
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}


	private String buildConsumerGroupRowKey(ConsumerGroup group)throws Exception{
		String topicId = group.getTopic().getId();
		StringBuilder sb = new StringBuilder(64);
		sb.append(topicId.substring(0, 8));
		sb.append(MQConstant.SEPARATOR);
		sb.append(group.buildObjectID());
		return sb.toString();
	}
	
	public int dropConsumerGroupByPK(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			
			MQTopic topic = getTopicByPK(group.getTopic().getId());
			int queueNum = topic.getQueueNum();
			MQueue queue = null;
			OutPosition position = null;
			for(int i=0;i<queueNum;i++) {
				queue = new MQueue(topic.getId(), i);
				queue.buildQueueId();
				position = new OutPosition();
				deleteOutPositionByPK(position.buildId(queue.getId(), group.getName()));
			}
			
			conn = dataSource.getConnection();
			tb = conn.getTable(consumerGroupTableName);
			byte[] rowKey =Bytes.toBytes(buildConsumerGroupRowKey(group));
			Delete del = new Delete(rowKey);
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public ConsumerGroup getConsumerGroupByPK(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(consumerGroupTableName);
			byte[] rowKey =Bytes.toBytes(buildConsumerGroupRowKey(group));
			Get get = new Get(rowKey);
			Result rt = tb.get(get);
			ConsumerGroup cgroup = null;
			if(rt!=null && rt.size()>0) {
				cgroup = new ConsumerGroup();
				cgroup.setName(Bytes.toString(rt.getValue(CF, ConsumerGroupTable.FIELD_NAME)));
				MQTopic topic = new MQTopic();
				topic.setId(Bytes.toString(rt.getValue(CF, ConsumerGroupTable.FIELD_TOPIC_ID)));
				cgroup.setTopic(topic);
				byte[] data = rt.getValue(CF, ConsumerGroupTable.FIELD_DOMAIN_ID);
				if(data!=null && data.length>0) {
					MQDomain domain = new MQDomain(Bytes.toLong(data));
					topic.setDomain(domain);
				}
				return cgroup;
			}else {
				return null;
			}
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public List<ConsumerGroup> getConsumerGroupList(String topicId) throws Exception {
		if(topicId==null || topicId.trim().length()==0) {
			return null;
		}
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(consumerGroupTableName);
			Scan scan = new Scan();
			scan.setRowPrefixFilter(Bytes.toBytes(topicId.substring(0, 8)));
			ResultScanner rs = tb.getScanner(scan);
			if(rs==null) {
				return null;
			}
			List<ConsumerGroup> groupList = new ArrayList<ConsumerGroup>();
			ConsumerGroup group = null;
			Result r = null;
			MQTopic topic = null;
			MQDomain domain = null;
			byte[] data = null;
			while((r = rs.next())!=null) {
				topic = new MQTopic(Bytes.toString(r.getValue(CF, ConsumerGroupTable.FIELD_TOPIC_ID)));
				if(!topicId.equals(topic.getId())) {
					continue;
				}
				data = r.getValue(CF, ConsumerGroupTable.FIELD_DOMAIN_ID);
				if(data!=null && data.length>0) {
					domain = new MQDomain(Bytes.toLong(r.getValue(CF, ConsumerGroupTable.FIELD_DOMAIN_ID)));
					topic.setDomain(domain);
				}
				group = new ConsumerGroup();
				group.setTopic(topic);
				group.setName(Bytes.toString(r.getValue(CF, ConsumerGroupTable.FIELD_NAME)));
				groupList.add(group);
			}
			
			return groupList;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	private String buildTopicUserKey(String topicId,String type,String user,String groupName)throws Exception{
		if(topicId==null || user==null || type==null) {
			throw new Exception("topic id or user or type is null");
		}
		StringBuilder sb = new StringBuilder(100);
		sb.append(topicId);
		sb.append(MQConstant.SEPARATOR);
		sb.append(type);
		sb.append(MQConstant.SEPARATOR);
		sb.append(user);
		if(groupName!=null) {
			sb.append(MQConstant.SEPARATOR);
			sb.append(groupName);
		}
		String id = StringUtil.generateMD5String(sb.toString());
		sb = new StringBuilder(100);
		sb.append(topicId.substring(0, 8));
		sb.append(MQConstant.SEPARATOR);
		sb.append(type);
		sb.append(MQConstant.SEPARATOR);
		sb.append(id);
		return sb.toString();
	}
	
	public int grantUserToProducer(MQTopic topic) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String key =buildTopicUserKey(topic.getId(), TopicUserTable.TYPE_IN, topic.getUser().getName(), null);
			byte[] rowKey =Bytes.toBytes(key);
			Put pt = new Put(rowKey);
			pt.addColumn(CF, TopicUserTable.FIELD_TOPIC_ID, Bytes.toBytes(topic.getId()));
			pt.addColumn(CF, TopicUserTable.FIELD_TYPE, Bytes.toBytes(TopicUserTable.TYPE_IN));
			pt.addColumn(CF, TopicUserTable.FIELD_USER, Bytes.toBytes(topic.getUser().getName()));
			tb.put(pt);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	public int removeProducerUser(MQTopic topic) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String key =buildTopicUserKey(topic.getId(), TopicUserTable.TYPE_IN, topic.getUser().getName(), null);
			byte[] rowKey =Bytes.toBytes(key);
			Delete del = new Delete(rowKey);
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	
	
	public List<User> getProducerUserList(String topicId) throws Exception {
		if(topicId==null || topicId.trim().length()==0) {
			return null;
		}
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			StringBuilder sb = new StringBuilder(20);
			sb.append(topicId.substring(0, 8));
			sb.append(MQConstant.SEPARATOR);
			sb.append(TopicUserTable.TYPE_IN);
			Scan scan = new Scan();
			scan.setRowPrefixFilter(Bytes.toBytes(sb.toString()));
			ResultScanner rs = tb.getScanner(scan);
			if(rs==null) {
				return null;
			}
			List<User> userList = new ArrayList<User>();
			Result rt = null;
			String tid = null;
			String type = null;
			User user = null;
			while((rt = rs.next())!=null) {
				tid = Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_TOPIC_ID));
				if(!topicId.equals(tid)) {
					continue;
				}
				type = Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_TYPE));
				if(!TopicUserTable.TYPE_IN.equals(type)) {
					continue;
				}
				user = new User();
				user.setName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_USER)));
				userList.add(user);
			}
			return userList;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public User getProducerUserByPK(MQTopic topic) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String key =buildTopicUserKey(topic.getId(), TopicUserTable.TYPE_IN, topic.getUser().getName(), null);
			byte[] rowKey =Bytes.toBytes(key);
			
			Get get = new Get(rowKey);
			Result rt = tb.get(get);
			if(rt==null || rt.size()==0) {
				return null;
			}
			
			User user = new User();
			user.setName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_USER)));
			return user;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public int grantUserToConsumerGroup(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String topicId = group.getTopic().getId();
			String userName = group.getUser().getName();
			String key =buildTopicUserKey(topicId, TopicUserTable.TYPE_OUT, userName, group.getName());
			byte[] rowKey =Bytes.toBytes(key);
			Put pt = new Put(rowKey);
			pt.addColumn(CF, TopicUserTable.FIELD_TOPIC_ID, Bytes.toBytes(topicId));
			pt.addColumn(CF, TopicUserTable.FIELD_TYPE, Bytes.toBytes(TopicUserTable.TYPE_OUT));
			pt.addColumn(CF, TopicUserTable.FIELD_USER, Bytes.toBytes(userName));
			pt.addColumn(CF, TopicUserTable.FIELD_GROUP, Bytes.toBytes(group.getName()));
			tb.put(pt);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public int removeConsumerUser(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String topicId = group.getTopic().getId();
			String userName = group.getUser().getName();
			String key =buildTopicUserKey(topicId, TopicUserTable.TYPE_OUT, userName, group.getName());
			byte[] rowKey =Bytes.toBytes(key);
			Delete del = new Delete(rowKey);
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public User getConsumerUserByPK(ConsumerGroup group) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			String topicId = group.getTopic().getId();
			String userName = group.getUser().getName();
			String key =buildTopicUserKey(topicId, TopicUserTable.TYPE_OUT, userName, group.getName());
			byte[] rowKey =Bytes.toBytes(key);
			
			Get get = new Get(rowKey);
			Result rt = tb.get(get);
			if(rt==null || rt.size()==0) {
				return null;
			}
			
			User user = new User();
			user.setName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_USER)));
			user.setGroupName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_GROUP)));
			return user;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public List<User> getConsumerUserList(String topicId) throws Exception {
		if(topicId==null || topicId.trim().length()==0) {
			return null;
		}
		Connection conn = null;
		Table tb = null;
		try{	
			conn = dataSource.getConnection();
			tb = conn.getTable(topicUserTableName);
			StringBuilder sb = new StringBuilder(20);
			sb.append(topicId.substring(0, 8));
			sb.append(MQConstant.SEPARATOR);
			sb.append(TopicUserTable.TYPE_OUT);
			Scan scan = new Scan();
			scan.setRowPrefixFilter(Bytes.toBytes(sb.toString()));
			ResultScanner rs = tb.getScanner(scan);
			if(rs==null) {
				return null;
			}
			List<User> userList = new ArrayList<User>();
			Result rt = null;
			String tid = null;
			String type = null;
			User user = null;
			while((rt = rs.next())!=null) {
				tid = Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_TOPIC_ID));
				if(!topicId.equals(tid)) {
					continue;
				}
				type = Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_TYPE));
				if(!TopicUserTable.TYPE_OUT.equals(type)) {
					continue;
				}
				user = new User();
				user.setName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_USER)));
				user.setGroupName(Bytes.toString(rt.getValue(CF, TopicUserTable.FIELD_GROUP)));
				userList.add(user);
			}
			return userList;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
	private byte[] copyByte(byte[] data, int offset, int len){
		byte[] v = new byte[len];
		v = Arrays.copyOfRange(data, offset, offset+len);
		return v;
	}
}
