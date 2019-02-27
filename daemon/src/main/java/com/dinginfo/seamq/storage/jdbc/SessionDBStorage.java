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
import java.util.Date;

import javax.sql.DataSource;

import com.dinginfo.seamq.common.DBUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.entity.User;

public class SessionDBStorage {
	public static final String BEAN_NAME="sessionStorage";
	
	private DataSource dataSource;

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	
	public MQSession create(MQSession session) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="insert into t_session(ss_id,ss_user,ss_domain,ss_utype,ss_uptdt)values(?,?,?,?,?)";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, session.getId());
			pstmt.setString(2, session.getUser().getName());
			pstmt.setLong(3, session.getUser().getDomain().getId());
			pstmt.setString(4, session.getUser().getType());
			Date date = session.getUpdatedTime();
			Timestamp ts = new Timestamp(date.getTime());
			pstmt.setTimestamp(5, ts);
			pstmt.executeUpdate();
			return session;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int update(MQSession session) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_session set ss_uptdt=? where ss_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			Date date = session.getUpdatedTime();
			Timestamp ts = new Timestamp(date.getTime());
			pstmt.setTimestamp(1, ts);
			pstmt.setString(2, session.getId());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}		
	}

	
	public int deleteByPK(String sessionId) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="delete from t_session where ss_id=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, sessionId);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public MQSession getByPK(String sessionId) throws Exception {
		MQSession  session =null;
		String sql ="select * from  t_session where ss_id=?";
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, sessionId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				session = new MQSession();
				session.setId(rs.getString("ss_id"));
				User user = new User();
				user.setName(rs.getString("ss_user"));
				user.setType(rs.getString("ss_utype"));
				user.setDomain(new MQDomain(rs.getLong("ss_domain")));
				session.setUser(user);
				Timestamp timestamp = rs.getTimestamp("ss_uptdt");
				if(timestamp!=null){
					Date date = new Date(timestamp.getTime());
					session.setUpdatedTime(date);	
				}
				
			}	
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return session;
	}
	
	
	public int clearOldSession(long timeout) throws Exception {
		String sql ="select * from  t_session";
		Connection conn = null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		int result =0;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long time =System.currentTimeMillis();
			String sessionId=null;
			while(rs.next()){
				Timestamp timestamp = rs.getTimestamp("ss_uptdt");
				if(timestamp!=null){
					time = System.currentTimeMillis() - timestamp.getTime();
					if(time>timeout){
						sessionId =rs.getString("ss_id");
						deleteByPK(sessionId);
						result++;
					}
				}
				
			}	
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return result;
	}

}
