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
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import com.dinginfo.seamq.common.DBUtil;
import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.User;

public class UserDBStorage {
	public static final String BEAN_NAME = "userStorage";
	
	private DataSource dataSource;
	

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	
	public void createUser(User user) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="insert into t_users(us_oid,us_domain,us_name,us_status,us_type,us_crtdt,us_uptdt,us_remark)values(?,?,?,?,?,?,?,?)";
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			int index=1;
			String objId = user.buildUserObjectID();
			pstmt.setString(index++, objId);
			pstmt.setLong(index++, user.getDomain().getId());
			pstmt.setString(index++, user.getName());
			pstmt.setString(index++, user.getStatus());
			pstmt.setString(index++, user.getType());
			pstmt.setTimestamp(index++, new Timestamp(user.getCreatedTime().getTime()));
			pstmt.setTimestamp(index++, new Timestamp(user.getUpdatedTime().getTime()));
			pstmt.setString(index++, user.getRemark());
			int n=pstmt.executeUpdate();
			createPassword(conn, objId, user.getPwd());
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}
	
	private int createPassword(Connection conn,String oid,String psw) throws Exception {
		PreparedStatement pstmt =null;
		String sql ="insert into t_user_psw(us_oid,us_psw)values(?,?)";
		try{
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, oid);
			pstmt.setString(2, psw);
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeStatement(pstmt);
		}
	}

	
	public int updateUserPassword(User user) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_user_psw set us_pwd=? where us_oid=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user.getPwd());
			pstmt.setString(3, user.buildUserObjectID());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int updateUser(User user) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt =null;
		String sql ="update t_users set us_status=?,us_remark=?,us_uptdt =? where us_oid=?";
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user.getStatus());
			pstmt.setString(2, user.getRemark());
			pstmt.setTimestamp(3, new Timestamp(user.getUpdatedTime().getTime()));
			pstmt.setString(4, user.buildUserObjectID());
			return pstmt.executeUpdate();
		}finally{
			DBUtil.closeConnection(conn, pstmt, null);
		}
	}

	
	public int deleteUserByPK(User user) throws Exception {

		int result =0;
		Connection conn = null;
		PreparedStatement pstmt =null;
		PreparedStatement pswstmt =null;
		String sql ="delete from t_users where us_oid=?";
		String sqlpsw = "delete from t_user_psw where us_oid=?";
		try {
			
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			String objId =user.buildUserObjectID();
			pstmt.setString(1, objId);
			
			result =pstmt.executeUpdate();
			
			pswstmt= conn.prepareStatement(sqlpsw);
			pswstmt.setString(1, objId);
			pswstmt.executeUpdate();
			
			
			conn.commit();

		} catch (Exception e) {
			conn.rollback();
			throw e;
		}finally{
			DBUtil.closeConnection(null, pswstmt, null);
			DBUtil.closeConnection(conn, pstmt, null);
		}
		return result;
	}

	
	public User getUserByPK(String userObjID) throws Exception {
		if(userObjID==null){
			return null;
		}
		String sql ="select * from  t_users where us_oid=?";
		Connection conn=null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		User u =null;
		Timestamp ts = null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, userObjID);
			rs = pstmt.executeQuery();
			if(rs.next()){
				u = new User();
				u.setName(rs.getString("us_name"));
				u.setType(rs.getString("us_type"));
				u.setRemark(rs.getString("us_remark"));
				u.setDomain(new MQDomain(rs.getLong("us_domain")));
				u.setStatus(rs.getString("us_status"));
				ts = rs.getTimestamp("us_crtdt");
				if(ts!=null){
					u.setCreatedTime(ts);	
				}
				ts = rs.getTimestamp("us_uptdt");
				if(ts!=null){
					u.setUpdatedTime(ts);
				}
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return u;
	}

	
	public User getUserByPK(User user) throws Exception {
		if(user==null){
			return null;
		}
		String userObjectID = user.buildUserObjectID();
		return getUserByPK(userObjectID);
	}
	
	
	public long getUserCount(long domainId) throws Exception {
		
		String sql ="select count(*) from  t_users where us_domain=?";
		Connection conn=null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		int n=0;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, domainId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				n = rs.getInt(1);
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return n;
	}

	
	public String getPassword(User user) throws Exception {
		if(user==null){
			return null;
		}
		String sql ="select * from  t_user_psw where us_oid=?";
		Connection conn=null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		String psw = null;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			String userObjID  = user.buildUserObjectID();
			pstmt.setString(1, userObjID);
			rs = pstmt.executeQuery();
			if(rs.next()){
				psw = rs.getString("us_psw");
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return psw;
	}

	
	public List<User> getUserList(long domainId, int pageNo,int pageSize) throws Exception {
		List<User> userList = new ArrayList<User>();
		String sql ="select * from  t_users where us_domain=? limit ? offset ?";
		Connection conn =null;
		PreparedStatement pstmt =null;
		ResultSet rs =null;
		User user =null;
		Timestamp time = null;
		long offset =pageSize * pageNo;
		try{
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, domainId);
			pstmt.setInt(2, pageSize);
			pstmt.setLong(3, offset);
			rs = pstmt.executeQuery();
			while(rs.next()){
				user = new User();
				user.setName(rs.getString("us_name"));
				user.setStatus(rs.getString("us_status"));
				user.setDomain(new MQDomain(rs.getLong("us_domain")));
				user.setRemark(rs.getString("us_remark"));
				user.setType(rs.getString("us_type"));
				time  = rs.getTimestamp("us_crtdt");
				if(time!=null){
					user.setCreatedTime(new Date(time.getTime()));
				}
				time = rs.getTimestamp("us_uptdt");
				if(time!=null){
					user.setUpdatedTime(new Date(time.getTime()));
				}
				userList.add(user);
			}
		}catch(Exception e){
			throw e;
		}finally{
			DBUtil.closeConnection(conn, pstmt, rs);
		}
		return userList;
	}


}
