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

package com.dinginfo.seamq.storage.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.MQMessageExt;
import com.dinginfo.seamq.MessageStatus;
import com.dinginfo.seamq.common.DBUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.storage.AttributeUtils;
import com.dinginfo.seamq.storage.MQKeyValue;


public class MessageDBStorage {
	public static final String BEAN_NAME = "messageStorage";
	
	private DataSource dataSource;

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	
	public MessageStatus put(MQMessageExt message) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		PreparedStatement pstmtOffset =null;
		MessageStatus msgStatus = null;
		String messageId = message.buildMessageId();
		StringBuilder sb = new StringBuilder(300);
		sb.append("insert into t_message_");
		sb.append(getTableIndex(messageId));
		sb.append("(ms_id,tp_id,ms_version,ms_data,ms_domain,ms_queue,ms_arraysize,ms_arrayidx,ms_offset,ms_attribute)values(?,?,?,?,?,?,?,?,?,?)");
		String sql =sb.toString();
		String sqlOffset = "update t_queue set qu_inoffset = qu_inoffset +1 where qu_id = ? and qu_inoffset=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			
			
			pstmtOffset = conn.prepareStatement(sqlOffset);
			pstmtOffset.setString(1, message.getQueue().getId());
			pstmtOffset.setLong(2, message.getOffset());
			int n =pstmtOffset.executeUpdate();
			if(n==0){
				msgStatus = new MessageStatus(MessageStatus.ERR_OFFSET);
				conn.rollback();
				return msgStatus;
			}
			
			pstmt = conn.prepareStatement(sql);
			
			pstmt.setString(1, messageId);
			pstmt.setString(2, message.getTopic().getId());
			pstmt.setString(3, message.getVersion());
			pstmt.setString(4, Base64.encodeBase64String(message.getData()));
			pstmt.setLong(5, message.getDomain().getId());
			pstmt.setInt(6, message.getQueue().getQueue());
			pstmt.setInt(7, message.getArraySize());
			pstmt.setInt(8, message.getArrayIndex());
			pstmt.setLong(9, message.getOffset());
			String jsonString =AttributeUtils.toJsonString(message.getAttributeList());
			if(jsonString==null || jsonString.trim().length()==0){
				jsonString ="";
			}
			pstmt.setString(10, jsonString);
			pstmt.executeUpdate();
			conn.commit();
			message.setMessageId(messageId);
			msgStatus = new MessageStatus(MessageStatus.OK);
			return msgStatus;
		} catch (Exception e) {
			conn.rollback();
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}finally{
			DBUtil.closeConnection(null, pstmtOffset, null);
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public MQMessageExt get(MQMessageExt message) throws Exception {

		MQMessageExt msg =null;
		StringBuilder sb = new StringBuilder(200);
		sb.append("select * from  t_message_");
		sb.append(getTableIndex(message.getMessageId()));
		sb.append(" where ms_id=?");
		String sql =sb.toString();
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, message.getMessageId());
			rs = pstmt.executeQuery();
			if(rs.next()){
				msg = new MQMessageExt();
				msg.setData(Base64.decodeBase64(rs.getString("ms_data")));
				msg.setDomain(new MQDomain(rs.getLong("ms_domain")));
				msg.setTopic(new MQTopic(rs.getString("tp_id")));
				MQueue queue  = new MQueue();
				queue.setQueue(rs.getInt("ms_queue"));
				msg.setQueue(queue);
				msg.setVersion(rs.getString("ms_version"));
				msg.setArraySize(rs.getInt("ms_arraysize"));
				msg.setArrayIndex(rs.getInt("ms_arrayidx"));
				msg.setOffset(rs.getLong("ms_offset"));
				String jsonString = rs.getString("ms_attribute");
				if(jsonString!=null && jsonString.trim().length()>0){
					List<MQKeyValue> kvList = JSON.parseArray(jsonString, MQKeyValue.class);
					if(kvList!=null && kvList.size()>0){
						for(MQKeyValue kv : kvList){
							msg.putAttribute(kv.getK(), new String(kv.getV()));
						}
					}
				}
				
			}	
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return msg;
	
	}

	
	public int delete(MQMessageExt message) throws Exception {
		int result =0;
		StringBuilder sb = new StringBuilder(200);
		sb.append("delete from t_message_");
		sb.append(getTableIndex(message.getMessageId()));
		sb.append(" where ms_id=?");
		String sql =sb.toString();
		String sqlOffset = "update t_customer set cu_outoffset = cu_outoffset + 1 where cu_id =? and cu_outoffset=?";
		
		Connection conn = null;
		PreparedStatement pstmt =null;
		PreparedStatement pstmtOffset =null;
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmtOffset = conn.prepareStatement(sqlOffset);
			pstmtOffset.setString(1, message.getQueue().getId());
			pstmtOffset.setLong(2, message.getOffset());
			result=pstmtOffset.executeUpdate();
			if(result==0){
				conn.rollback();
				return 0;
			}
			
			pstmt = conn.prepareStatement(sql);
			String id = message.getMessageId();
			pstmt.setString(1, id);
			pstmt.executeUpdate();
			conn.commit();

		} catch (Exception e) {
			conn.rollback();
			e.printStackTrace();
		}finally{
			DBUtil.closeConnection(null, pstmtOffset, null);
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}
	
	private String getTableIndex(String messageId) {
		if(messageId==null) {
			return "0";
		}
		int index = messageId.hashCode() % DBConstant.TABLE_SIZE;
		if(index<0) {
			return String.valueOf(Math.abs(index));
		}else {
			return String.valueOf(index);
		}
	}

}
