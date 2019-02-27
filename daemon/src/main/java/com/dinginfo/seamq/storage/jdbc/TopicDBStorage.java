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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.dinginfo.seamq.common.DBUtil;
import com.dinginfo.seamq.entity.ConsumerGroup;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQTopic;
import com.dinginfo.seamq.entity.MQueue;
import com.dinginfo.seamq.entity.OutPosition;
import com.dinginfo.seamq.entity.User;

public class TopicDBStorage {
	public static final String BEAN_NAME = "topicStorage";
	
	private static final String TYPE_IN = "IN";
	
	private static final String TYPE_OUT = "OUT";
	
	private DataSource dataSource;

	

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	
	public void createTopic(MQTopic topic) throws Exception {

		Connection conn = null;
		PreparedStatement pstmt =null;
		int m =0;
		String sql ="insert into t_topic(tp_id,tp_name,tp_qnum,tp_remark,tp_crtdt,tp_uptdt,tp_domain)values(?,?,?,?,?,?,?)";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getId());
			pstmt.setString(2, topic.getName());
			pstmt.setInt(3, topic.getQueueNum());
			pstmt.setString(4, topic.getRemark());
			Date date = Calendar.getInstance().getTime();
			pstmt.setTimestamp(5, new Timestamp(date.getTime()));
			pstmt.setTimestamp(6, new Timestamp(date.getTime()));
			pstmt.setLong(7, topic.getDomain().getId());
			m=pstmt.executeUpdate();
			
			//for consumer group
			ConsumerGroup group = new ConsumerGroup();
			group.setTopic(topic);
			group.setName(ConsumerGroup.DEFAULT_GROUP_NAME);
			insertConsumerGroup(conn, group);
			//
			MQueue queue = null;
			OutPosition position = null;
			for(int i=0,n = topic.getQueueNum();i<n;i++){
				queue = new MQueue(topic.getId(),i);
				queue.buildQueueId();
				queue.setInOffset(0);
				queue.setDomain(topic.getDomain());
				queue.setCreatedTime(date);
				insertQueue(conn, queue);
				//
				position = new OutPosition();
				position.setId(queue.getId());
				position.setOutOffset(0L);
				position.setQueueId(queue.getId());
				position.setTopicId(topic.getId());
				insertOutPosition(conn, position);
				
				
				position = new OutPosition();
				position.setId(position.buildId(queue.getId(), ConsumerGroup.DEFAULT_GROUP_NAME));
				position.setOutOffset(0L);
				position.setQueueId(queue.getId());
				position.setGroupName(ConsumerGroup.DEFAULT_GROUP_NAME);
				position.setTopicId(topic.getId());
				insertOutPosition(conn, position);

			}
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	private int insertQueue(Connection conn,MQueue queue)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "insert into t_queue(qu_id,qu_topicid,qu_queue,qu_crtdt,qu_inoffset,qu_domain)values(?,?,?,?,?,?)";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queue.getId());
			pstmt.setString(2, queue.getTopic().getId());
			pstmt.setInt(3, queue.getQueue());
			pstmt.setTimestamp(4, new Timestamp(queue.getCreatedTime().getTime()));
			pstmt.setLong(5, queue.getInOffset());
			pstmt.setLong(6, queue.getDomain().getId());
			return pstmt.executeUpdate();
		}finally {
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private void insertConsumerGroup(Connection conn,ConsumerGroup group)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "insert into t_consumergroup(gp_domain,gp_topicid,gp_name)values(?,?,?)";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, group.getTopic().getDomain().getId());
			pstmt.setString(2, group.getTopic().getId());
			pstmt.setString(3, group.getName());
			pstmt.executeUpdate();
		}finally {
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private void insertOutPosition(Connection conn,OutPosition position)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "insert into t_outposition(op_id,op_outoffset,op_queueid,op_group,op_topicid)values(?,?,?,?,?)";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, position.getId());
			pstmt.setLong(2, position.getOutOffset());
			pstmt.setString(3, position.getQueueId());
			pstmt.setString(4, position.getGroupName());
			pstmt.setString(5, position.getTopicId());
			pstmt.executeUpdate();
		}finally {
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private void deleteOutPositionByPK(Connection conn,OutPosition position)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "delete from t_outposition where op_id=?";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, position.getId());
			pstmt.executeUpdate();
		}finally {
			DBUtil.closeStatement(pstmt);
		}
	}
	
	public int updateTopic(MQTopic topic) throws Exception {
		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_topic set tp_remark=? where tp_id=?";
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getRemark());
			pstmt.setString(2, topic.getId());
			result =pstmt.executeUpdate();
		} catch (Exception e) {
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
		
	}

	
	public long getTopicCount(long domainId) throws Exception {
		String sql ="select count(*) from  t_topic where tp_domain=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		long result =0;
		
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, domainId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				result = rs.getLong(1);
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return result;
	}

	
	public List<MQTopic> getTopicList(long domainId, int pageNo,int pageSize) throws Exception {
		List<MQTopic> topicList = new ArrayList<MQTopic>();
		String sql ="select * from  t_topic where tp_domain=? limit ? offset ?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		MQTopic topic =null;
		Timestamp time = null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, domainId);
			pstmt.setInt(2, pageSize);
			pstmt.setLong(3, pageNo);
			rs = pstmt.executeQuery();
			while(rs.next()){
				topic = new MQTopic();
				topic.setId(rs.getString("tp_id"));
				topic.setName(rs.getString("tp_name"));
				topic.setQueueNum(rs.getInt("tp_qnum"));
				topic.setRemark(rs.getString("tp_remark"));
				topic.setDomain(new MQDomain(rs.getLong("tp_domain")));
				topic.setStatus(rs.getString("tp_status"));
				time  = rs.getTimestamp("tp_crtdt");
				if(time!=null){
					topic.setCreatedTime(new Date(time.getTime()));
				}
				time = rs.getTimestamp("tp_uptdt");
				if(time!=null){
					topic.setUpdatedTime(new Date(time.getTime()));
				}
				topicList.add(topic);
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return topicList;
	
	}

	
	public int disableTopic(MQTopic topic) throws Exception {
		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_topic set tp_status=? where tp_id=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, MQTopic.STATUS_DISABLE);
			pstmt.setString(2, topic.getId());
			result =pstmt.executeUpdate();
			updateQueueStatusByTopic(conn,topic.getId(),MQTopic.STATUS_DISABLE);
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	
	}
	
	private int updateQueueStatusByTopic(Connection conn,String topicId,String status) throws Exception {
		PreparedStatement pstmt =null;
		String sql ="update t_queue set qu_status=? where qu_topicid=?";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.setString(2, topicId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}

	
	public int dropTopic(MQTopic topic) throws Exception {
		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="delete from t_topic where tp_id=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getId());
			result =pstmt.executeUpdate();
	
			deleteQueueByTopic(conn, topic.getId());
			deleteUserOfTopic(conn, topic.getId());
			deleteOutPositionByTopic(conn, topic.getId());
			deleteConsumerGroupByTopic(conn, topic.getId());
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}
	
	private int deleteUserOfTopic(Connection conn,String topicId)throws Exception{
		PreparedStatement pstmt =null;
		String sql ="delete from t_topic_user where tu_topicid=?";
		try{
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private int deleteQueueByTopic(Connection conn,String topicId)throws Exception{
		PreparedStatement pstmt =null;
		String sql ="delete from t_queue where qu_topicid=?";
		try{
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private int deleteConsumerGroupByTopic(Connection conn,String topicId)throws Exception{
		PreparedStatement pstmt =null;
		String sql ="delete from t_consumergroup where gp_topicid =?";
		try{
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}
	
	private int deleteOutPositionByTopic(Connection conn,String topicId)throws Exception{
		PreparedStatement pstmt =null;
		String sql ="delete from t_outposition where op_topicid =?";
		try{
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}

	
	public MQTopic getTopicByPK(String key) throws Exception {
		String sql ="select * from  t_topic where tp_id=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		MQTopic topic =null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, key);
			rs = pstmt.executeQuery();
			if(rs.next()){
				topic = new MQTopic();
				topic.setId(rs.getString("tp_id"));
				topic.setName(rs.getString("tp_name"));
				topic.setQueueNum(rs.getInt("tp_qnum"));
				topic.setRemark(rs.getString("tp_remark"));
				topic.setDomain(new MQDomain(rs.getLong("tp_domain")));
				topic.setStatus(rs.getString("tp_status"));
				Timestamp ts =rs.getTimestamp("tp_crtdt");
				if(ts!=null){
					Date d = new Date(ts.getTime());
					topic.setCreatedTime(d);
				}
				ts =rs.getTimestamp("tp_uptdt");
				if(ts!=null){
					Date d = new Date(ts.getTime());
					topic.setUpdatedTime(d);
				}
				
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return topic;
	}

	
	public MQueue loadQueueByPK(String queueId) throws Exception {
		String sql ="select * from  t_queue where qu_id=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		MQueue queue =null;
		OutPosition position = null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queueId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				queue = new MQueue(queueId);
				queue.setTopic(new MQTopic(rs.getString("qu_topicid")));
				queue.setQueue(rs.getInt("qu_queue"));
				queue.setDomain(new MQDomain(rs.getLong("qu_domain")));
				queue.setInOffset(rs.getLong("qu_inoffset"));
				queue.setStatus(rs.getString("qu_status"));
				Timestamp ts =rs.getTimestamp("qu_crtdt");
				if(ts!=null){
					Date d = new Date(ts.getTime());
					queue.setCreatedTime(d);
				}
				position = getOutPositionByPK(new OutPosition(queueId));
				if(position!=null) {
					queue.setOutOffset(position.getOutOffset());
				}
			}
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return queue;
	}
	
	public MQueue getQueueByPK(String queueId) throws Exception {
		String sql ="select * from  t_queue where qu_id=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		MQueue queue =null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queueId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				queue = new MQueue(queueId);
				queue.setTopic(new MQTopic(rs.getString("qu_topicid")));
				queue.setQueue(rs.getInt("qu_queue"));
				queue.setDomain(new MQDomain(rs.getLong("qu_domain")));
				queue.setInOffset(rs.getLong("qu_inoffset"));
				queue.setStatus(rs.getString("qu_status"));
				Timestamp ts =rs.getTimestamp("qu_crtdt");
				if(ts!=null){
					Date d = new Date(ts.getTime());
					queue.setCreatedTime(d);
				}
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return queue;
	}

	public OutPosition getOutPositionByPK(OutPosition position)throws Exception{
		String sql ="select * from  t_outposition where op_id=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		OutPosition bean =null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, position.getId());
			rs = pstmt.executeQuery();
			if(rs.next()){
				bean = new OutPosition();
				bean.setId(position.getId());
				bean.setOutOffset(rs.getLong("op_outoffset"));
				bean.setGroupName(rs.getString("op_group"));
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return bean;
	}
	
	
	public int updateQueueInOffset(MQueue queue) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_queue set qu_inoffset=? where qu_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, queue.getInOffset());
			pstmt.setString(2, queue.getId());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int updateQueueOutOffset(MQueue queue) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_outposition set op_outoffset=? where op_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, queue.getOutOffset());
			pstmt.setString(2, queue.getId());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public int increaseOutPosition(OutPosition position) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_outposition set op_outoffset=op_outoffset+1 where op_id=? and op_outoffset=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, position.getId());
			pstmt.setLong(2, position.getOutOffset());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}


	
	public int enableTopic(MQTopic topic) throws Exception {
		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_topic set tp_status=? where tp_id=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "");
			pstmt.setString(2, topic.getId());
			result =pstmt.executeUpdate();
			updateQueueStatusByTopic(conn, topic.getId(), "");
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}

	
	public int disableQueue(String queueId) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update  t_queue set qu_status=? where qu_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, MQTopic.STATUS_DISABLE);
			pstmt.setString(2, queueId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int enableQueue(String queueId) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update  t_queue set qu_status=? where qu_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "");
			pstmt.setString(2, queueId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int increaseQueue(MQTopic topic, int queueNum,List<String> customerList) throws Exception {
		int result =queueNum;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_topic set tp_qnum=? where tp_id=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, topic.getQueueNum()+queueNum);
			pstmt.setString(2, topic.getId());
			pstmt.executeUpdate();
			
			Date date = Calendar.getInstance().getTime();
			MQueue queue = null; 
			OutPosition position = new OutPosition();
			for(int i=topic.getQueueNum(),n = topic.getQueueNum()+queueNum;i<n;i++){
				queue = new MQueue(topic.getId(),i);
				queue.buildQueueId();
				queue.setQueue(i);
				queue.setCreatedTime(date);
				queue.setDomain(topic.getDomain());
				queue.setInOffset(0);
				insertQueue(conn, queue);
				
				//queue out_offset
				position = new OutPosition();
				position.setId(queue.getId());
				position.setOutOffset(0);
				position.setQueueId(queue.getId());
				position.setGroupName(null);
				position.setTopicId(topic.getId());
				insertOutPosition(conn, position);
				
				
				if(customerList!=null) {
					for(String cus : customerList) {
						position = new OutPosition();
						position.setId(position.buildId(queue.getId(), cus));
						position.setOutOffset(0);
						position.setQueueId(queue.getId());
						position.setGroupName(cus);
						position.setTopicId(topic.getId());
						insertOutPosition(conn, position);
					}
					
				}
			}

			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}

	
	public int reduceQueue(MQTopic topic, String queueId) throws Exception {

		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_topic set tp_qnum=? where tp_id=?";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, topic.getQueueNum()-1);
			pstmt.setString(2, topic.getId());
			result =pstmt.executeUpdate();
	
			deleteQueueByPK(conn, queueId);
			deleteOutPositionByQueue(conn, queueId);
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}
	
	private int deleteOutPositionByQueue(Connection conn,String queueId)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "delete from t_outposition where op_queueid =?";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queueId);
			return pstmt.executeUpdate();
		} finally{
			DBUtil.closeStatement(pstmt);
		}
	}

	private int deleteQueueByPK(Connection conn,String queueId)throws Exception{
		PreparedStatement pstmt =null;
		String sql = "delete from t_queue where qu_id=?";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queueId);
			return pstmt.executeUpdate();
		} finally{
			DBUtil.closeStatement(pstmt);
		}
	}
	
	public List<OutPosition> getOutPositionList(String queueId)throws Exception{
		String sql ="select * from  t_outposition where op_queueid=?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		OutPosition position= null;
		List<OutPosition> positionList = new ArrayList<OutPosition>();
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, queueId);
			rs = pstmt.executeQuery();
			String groupName = null;
			while(rs.next()){
				groupName = rs.getString("op_group");
				if(groupName==null || groupName.trim().length()==0) {
					continue;
				}
				position = new OutPosition();
				position.setId(rs.getString("op_id"));
				position.setOutOffset(rs.getLong("op_outoffset"));
				position.setGroupName(rs.getString("op_group"));
				positionList.add(position);
			}
			
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return positionList;
	}
	
	public void createConsumerGroup(ConsumerGroup group)throws Exception{
		Connection conn = null;
		try{
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			insertConsumerGroup(conn, group);
			MQTopic topic = getTopicByPK(group.getTopic().getId());
			OutPosition position = null;
			MQueue queue = null;
			int n = topic.getQueueNum();
			for(int i=0;i<n;i++) {
				queue = new MQueue(topic.getId(), i);
				queue.buildQueueId();
				position = new OutPosition();
				position.setTopicId(topic.getId());
				position.setGroupName(group.getName());
				position.setQueueId(queue.getId());
				position.setOutOffset(0);
				position.setId(position.buildId(queue.getId(), group.getName()));
				insertOutPosition(conn, position);
			}
			conn.commit();
		}catch(Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn);
		}
	}
	
	public ConsumerGroup getConsumerGroupByPK(ConsumerGroup group)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_consumergroup where gp_topicid=? and gp_name=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, group.getTopic().getId());
			pstmt.setString(2, group.getName());
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			if(rs.next()) {
				ConsumerGroup congroup = new ConsumerGroup();
				congroup.setName(rs.getString("gp_name"));
				MQTopic topic = new MQTopic(rs.getString(rs.getString("gp_topicid")));
				MQDomain domain = new MQDomain(rs.getLong("gp_domain"));
				topic.setDomain(domain);
				congroup.setTopic(topic);
				return congroup;
			}else {
				return null;
			}
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}
	
	public int dropConsumerGroupByPK(ConsumerGroup group)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		int result =0;
		String sql ="delete from t_consumergroup where gp_topicid=? and gp_name=?";
		try{
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			MQTopic topic = getTopicByPK(group.getTopic().getId());
			MQueue queue = null;
			OutPosition position = null;
			int n = topic.getQueueNum();
			for(int i=0;i<n;i++) {
				queue = new MQueue(topic.getId(), i);
				queue.buildQueueId();
				position = new OutPosition();
				position.setId(position.buildId(queue.getId(), group.getName()));
				deleteOutPositionByPK(conn, position);
			}
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, group.getTopic().getId());
			pstmt.setString(2, group.getName());
			result=pstmt.executeUpdate();
			conn.commit();
			return result;
		}catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public List<ConsumerGroup> getConsumerGroupList(String topicId)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_consumergroup where gp_topicid=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			List<ConsumerGroup> groupList = new ArrayList<ConsumerGroup>();
			ConsumerGroup congroup = null;
			while(rs.next()) {
				congroup = new ConsumerGroup();
				congroup.setName(rs.getString("gp_name"));
				MQTopic topic = new MQTopic(rs.getString("gp_topicid"));
				MQDomain domain = new MQDomain(rs.getLong("gp_domain"));
				topic.setDomain(domain);
				congroup.setTopic(topic);
				groupList.add(congroup);
			}
			return groupList;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}
	
	public int grantUserToProducer(MQTopic topic)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="insert into t_topic_user(tu_topicid,tu_type,tu_user,tu_group)values(?,?,?,?)";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getId());
			pstmt.setString(2, TYPE_IN);
			pstmt.setString(3, topic.getUser().getName());
			pstmt.setString(4, ConsumerGroup.DEFAULT_GROUP_NAME);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public int removeProducerUser(MQTopic topic)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="delete from t_topic_user where tu_topicid=? and tu_type=? and tu_user=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getId());
			pstmt.setString(2, TYPE_IN);
			pstmt.setString(3, topic.getUser().getName());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public User getProducerUserByPK(MQTopic topic)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_topic_user where tu_topicid=? and tu_type=? and tu_user=? and tu_group=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topic.getId());
			pstmt.setString(2, TYPE_IN);
			pstmt.setString(3, topic.getUser().getName());
			pstmt.setString(4, ConsumerGroup.DEFAULT_GROUP_NAME);
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			if(rs.next()) {
				User user = new User();
				user.setName(rs.getString("tu_user"));
				return user;
			}else {
				return null;
			}
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}
	
	public List<User> getProducerUserList(String topicId)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_topic_user where tu_topicid=? and tu_type=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			pstmt.setString(2, TYPE_IN);
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			List<User> userList = new ArrayList<User>();
			User user = null;
			while(rs.next()) {
				user = new User();
				user.setName(rs.getString("tu_user"));
				userList.add(user);
			}
			return userList;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}
	
	public int grantUserToConsumerGroup(ConsumerGroup group)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="insert into t_topic_user(tu_topicid,tu_type,tu_user,tu_group)values(?,?,?,?)";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, group.getTopic().getId());
			pstmt.setString(2, TYPE_OUT);
			pstmt.setString(3, group.getUser().getName());
			pstmt.setString(4, group.getName());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public int removeConsumerUser(ConsumerGroup group)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="delete from t_topic_user where tu_topicid=? and tu_type=? and tu_user=? and tu_group=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, group.getTopic().getId());
			pstmt.setString(2, TYPE_OUT);
			pstmt.setString(3, group.getUser().getName());
			pstmt.setString(4, group.getName());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	public User getConsumerUserByPK(ConsumerGroup group)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_topic_user where tu_topicid=? and tu_type=? and tu_user=? and tu_group=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, group.getTopic().getId());
			pstmt.setString(2, TYPE_OUT);
			pstmt.setString(3, group.getUser().getName());
			pstmt.setString(4, group.getName());
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			if(rs.next()) {
				User user = new User();
				user.setName(rs.getString("tu_user"));
				return user;
			}else {
				return null;
			}
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}
	
	public List<User> getConsumerUserList(String topicId)throws Exception{
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs = null;
		String sql ="select * from t_topic_user where tu_topicid=? and tu_type=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, topicId);
			pstmt.setString(2, TYPE_OUT);
			rs = pstmt.executeQuery();
			if(rs==null) {
				return null;
			}
			List<User> userList = new ArrayList<User>();
			User user = null;
			while(rs.next()) {
				user = new User();
				user.setGroupName(rs.getString("tu_group"));
				user.setName(rs.getString("tu_user"));
				userList.add(user);
			}
			return userList;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
	}

}
