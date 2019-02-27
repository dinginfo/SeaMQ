package com.dinginfo.seamq.storage.hbase;

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

import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MessageStatus;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.storage.AttributeUtils;
import com.dinginfo.seamq.storage.MQKeyValue;
import com.dinginfo.seamq.storage.MessageMeta;
import com.dinginfo.seamq.storage.hbase.mapping.OutPositionTable;
import com.dinginfo.seamq.storage.hbase.mapping.MessageTable;
import com.dinginfo.seamq.storage.hbase.mapping.QueueTable;

public class MessageHBaseStorage extends BasicHBaseStorage{
	public static final String BEAN_NAME = "messageStorage";
	
	private final byte[] CF = Bytes.toBytes("cf");

	private HBaseDataSource dataSource;
	
	private TableName messageTableName = null;
	
	private TableName queueTableName = null;
	
	private TableName outPositionTableName = null;
	
	public MessageHBaseStorage(String namespace){
		messageTableName = TableName.valueOf(HBaseUtils.buildTalbeName(namespace, MessageTable.TABLE_NAME));
		queueTableName = TableName.valueOf(HBaseUtils.buildTalbeName(namespace, QueueTable.TABLE_NAME));
		outPositionTableName = TableName.valueOf(HBaseUtils.buildTalbeName(namespace, OutPositionTable.TABLE_NAME));
	}
	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}

	
	public MessageStatus put(MQMessageExt message) throws Exception {

		MessageStatus msgStatus = null;
		Connection conn = null;
		Table tbQueue = null;
		Table tbMsg = null;
		MQueue queue = new MQueue(message.getQueue().getId());
		boolean successOffset =false;
		boolean sucessMessage =false;
		byte[] inQueueRowKey=null;
		try {
			conn = dataSource.getConnection();
			tbQueue = conn.getTable(queueTableName);
			tbMsg = conn.getTable(messageTableName);
			
			
			//for queue 
			
			inQueueRowKey =Bytes.toBytes(queue.getId());
			Put ptQueue = new Put(inQueueRowKey);
			ptQueue.addColumn(CF, QueueTable.FIELD_IN_OFFSET,
					Bytes.toBytes(message.getOffset() + 1));
			if(hbaseVersion==1) {
				successOffset = tbQueue.checkAndPut(inQueueRowKey, CF, QueueTable.FIELD_IN_OFFSET, Bytes.toBytes(message.getOffset()), ptQueue);	
			}else {
				successOffset = tbQueue.checkAndMutate(inQueueRowKey, CF).qualifier(QueueTable.FIELD_IN_OFFSET).ifEquals(Bytes.toBytes(message.getOffset())).thenPut(ptQueue);
			}
			
			if(!successOffset){
				msgStatus = new MessageStatus(MessageStatus.ERR_OFFSET);
				return msgStatus;
			}
			//for message info and body
			MessageMeta meta = new MessageMeta();
			meta.setDomainId(message.getDomain().getId());
			meta.setTopicId(message.getTopic().getId());
			meta.setQueue(message.getQueue().getQueue());
			meta.setVer(message.getVersion());
			meta.setArraySize(message.getArraySize());
			Date createdTime = message.getCreatedTime();
			meta.setCrtdt(createdTime.getTime());
			meta.setOffset(message.getOffset());
			String metaJsonString = JSON.toJSONString(meta);
			if(message.getMessageId()==null) {
				message.buildMessageId();
			}
			byte[] msgRowKey= Bytes.toBytes(message.getMessageId());
			Put ptMsg = new Put(msgRowKey);
			ptMsg.addColumn(CF, MessageTable.FIELD_META,Bytes.toBytes(metaJsonString));
			ptMsg.addColumn(CF, MessageTable.FIELD_DATA, message.getData());
			String attJsonString =AttributeUtils.toJsonString(message.getAttributeList());
			if(attJsonString!=null && attJsonString.trim().length()>0){
				ptMsg.addColumn(CF, MessageTable.FIELD_ATTRIBUTES, Bytes.toBytes(attJsonString));
			}
			
			if(hbaseVersion==1) {
				sucessMessage = tbMsg.checkAndPut(msgRowKey, CF, msgRowKey, null, ptMsg);	
			}else {
				sucessMessage = tbMsg.checkAndMutate(msgRowKey, CF).ifNotExists().thenPut(ptMsg);
			}
			
			if(successOffset && successOffset){
				msgStatus = new MessageStatus(MessageStatus.OK);
				return msgStatus;
			}else if(successOffset){
				ptQueue = new Put(inQueueRowKey);
				ptQueue.addColumn(CF, QueueTable.FIELD_IN_OFFSET,
						Bytes.toBytes(message.getOffset()));
				if(hbaseVersion==1) {
					tbQueue.checkAndPut(inQueueRowKey, CF, QueueTable.FIELD_IN_OFFSET, Bytes.toBytes(message.getOffset()+1), ptQueue);	
				}else {
					tbQueue.checkAndMutate(inQueueRowKey, CF).qualifier(QueueTable.FIELD_IN_OFFSET).ifEquals(Bytes.toBytes(message.getOffset()+1)).thenPut(ptQueue);
				}
				
				msgStatus = new MessageStatus(MessageStatus.ERROR);
				return msgStatus;
			}else{
				msgStatus = new MessageStatus(MessageStatus.ERROR);
				return msgStatus;
			}
			
		} catch (Exception e) {
			if(successOffset){
				Put ptQueue = new Put(inQueueRowKey);
				ptQueue.addColumn(CF, QueueTable.FIELD_IN_OFFSET,
						Bytes.toBytes(message.getOffset()));
				if(hbaseVersion==1) {
					tbQueue.checkAndPut(inQueueRowKey, CF, QueueTable.FIELD_IN_OFFSET, Bytes.toBytes(message.getOffset()+1), ptQueue);	
				}else {
					tbQueue.checkAndMutate(inQueueRowKey, CF).qualifier(QueueTable.FIELD_IN_OFFSET).ifEquals(Bytes.toBytes(message.getOffset()+1)).thenPut(ptQueue);
				}
				
			}
			throw e;
		}finally{
			HBaseUtils.closeTable(tbQueue);
			HBaseUtils.closeTable(tbMsg);
		}
	}

	
	public MQMessageExt get(MQMessageExt message) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(messageTableName);
			
			Get get = new Get(Bytes.toBytes(message.getMessageId()));
			Result rt = tb.get(get);
			if(rt==null){
				return null;			
				
			}
			
			List<Cell> cells = rt.listCells();
			if(cells==null || cells.size()==0){
				return null;
			}
			byte[] data =null;
			MQMessageExt msg =new MQMessageExt();
			msg.setMessageId(message.getMessageId());
			msg.setData(rt.getValue(CF, MessageTable.FIELD_DATA));
			data = rt.getValue(CF, MessageTable.FIELD_META);
			String metaJsonString = Bytes.toString(data);
			MessageMeta meta = JSON.parseObject(metaJsonString, MessageMeta.class);
			msg.setVersion(meta.getVer());
			msg.setDomain(new MQDomain(meta.getDomainId()));
			MQTopic topic = new MQTopic();
			topic.setId(meta.getTopicId());
			msg.setTopic(topic);
			MQueue queue = new MQueue();
			queue.setQueue(meta.getQueue());
			msg.setQueue(queue);;
			Date createdTime = new Date(meta.getCrtdt());
			msg.setCreatedTime(createdTime);
			msg.setOffset(meta.getOffset());
			data = rt.getValue(CF, MessageTable.FIELD_ATTRIBUTES);
			if(data!=null && data.length>0){
				String jsonString = Bytes.toString(data);
				List<MQKeyValue> kvList = JSON.parseArray(jsonString, MQKeyValue.class);
				if(kvList!=null && kvList.size()>0){
					for(MQKeyValue kv : kvList){
						msg.putAttribute(kv.getK(), new String(kv.getV()));
					}
				}
			}
			return msg;
		}finally{
			HBaseUtils.closeTable(tb);
		}

	}

	
	public void delete(MQMessageExt message) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(messageTableName);
			byte[] rowKey = Bytes.toBytes(message.getMessageId());
			Delete del = new Delete(rowKey);
			tb.delete(del);
			
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}
	
}
