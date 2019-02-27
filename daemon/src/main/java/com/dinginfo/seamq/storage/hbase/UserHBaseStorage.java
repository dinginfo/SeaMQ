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
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.storage.hbase.mapping.UserTable;

public class UserHBaseStorage {
	public static final String BEAN_NAME="userStorage";

	private final byte[] CF = Bytes.toBytes("cf");

	private HBaseDataSource dataSource;
	
	//private DomainStorage domainStorage;
	
	private String userTableName = null;
	
	public UserHBaseStorage(String namespace){
		userTableName = HBaseUtils.buildTalbeName(namespace, UserTable.TABLE_NAME);
	}

	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}	
	public void createUser(User user) throws Exception {

		Connection conn = null;
		Table tb = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			byte[] rowKey = Bytes.toBytes(user.buildUserObjectID());
			Put put = new Put(rowKey);
			put.addColumn(CF, UserTable.FIELD_NAME, Bytes.toBytes(user.getName()));
			put.addColumn(CF, UserTable.FIELD_DOMAIN, Bytes.toBytes(user.getDomain().getId()));
			put.addColumn(CF, UserTable.FIELD_PASSWORD, Bytes.toBytes(user.getPwd()));
			if (user.getType() != null) {
				put.addColumn(CF, UserTable.FIELD_TYPE, Bytes.toBytes(user.getType()));
			}
			if (user.getRemark() != null) {
				put.addColumn(CF, UserTable.FIELD_REMARK, Bytes.toBytes(user.getRemark()));
			}
			if (user.getCreatedTime() != null) {
				put.addColumn(CF, UserTable.FIELD_CREATED_TIME, Bytes.toBytes(user.getCreatedTime().getTime()));
			}
			if (user.getUpdatedTime() != null) {
				put.addColumn(CF, UserTable.FIELD_UPDATED_TIME, Bytes.toBytes(user.getUpdatedTime().getTime()));
			}
			if(user.getStatus()!=null) {
				put.addColumn(CF, UserTable.FIELD_STATUS, Bytes.toBytes(user.getStatus()));
			}
			tb.put(put);
		} finally{
			HBaseUtils.closeTable(tb);
		}

	}

	
	public int updateUser(User user) throws Exception {
		Connection conn = null;
		Table tb = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			Put put = new Put(Bytes.toBytes(user.buildUserObjectID()));
			if (user.getType() != null) {
				put.addColumn(CF, UserTable.FIELD_TYPE, Bytes.toBytes(user.getType()));
			}
			if (user.getRemark() != null) {
				put.addColumn(CF, UserTable.FIELD_REMARK, Bytes.toBytes(user.getRemark()));
			}
			if (user.getUpdatedTime() != null) {
				put.addColumn(CF, UserTable.FIELD_UPDATED_TIME, Bytes.toBytes(user.getUpdatedTime().getTime()));
			}
			if(user.getStatus()!=null){
				put.addColumn(CF, UserTable.FIELD_STATUS, Bytes.toBytes(user.getStatus()));
			}
			tb.put(put);
			return 1;
		} finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int updateUserPassword(User user) throws Exception {
		Connection conn = null;
		Table tb = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			String key = user.buildUserObjectID();
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(CF, UserTable.FIELD_PASSWORD, Bytes.toBytes(user.getPwd()));
			tb.put(put);
			return 1;
		} finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int deleteUserByPK(User user) throws Exception {
		Connection conn = null;
		Table tb = null;
		try {
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			String key = user.buildUserObjectID();
			Delete del = new Delete(Bytes.toBytes(key));
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public User getUserByPK(User user) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			String key = user.buildUserObjectID();
			Get get = new Get(Bytes.toBytes(key));
			Result rs = tb.get(get);
			if(rs==null || rs.size()==0){
				return null;
			}
			
			byte[] value = null;
			User u = new User();
			u.setName(Bytes.toString(rs.getValue(CF, UserTable.FIELD_NAME)));
			u.setDomain(new MQDomain(Bytes.toLong(rs.getValue(CF, UserTable.FIELD_DOMAIN))));
			u.setType(Bytes.toString(rs.getValue(CF, UserTable.FIELD_TYPE)));
			u.setRemark(Bytes.toString(rs.getValue(CF, UserTable.FIELD_REMARK)));
			u.setStatus(Bytes.toString(rs.getValue(CF, UserTable.FIELD_STATUS)));
			value = rs.getValue(CF, UserTable.FIELD_CREATED_TIME);
			if(value!=null){
				u.setCreatedTime(new Date(Bytes.toLong(value)));
			}
			value = rs.getValue(CF, UserTable.FIELD_UPDATED_TIME);
			if(value!=null){
				u.setUpdatedTime(new Date(Bytes.toLong(value)));
			}
			return u;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}
	
	public String getPassword(User user) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			String key = user.buildUserObjectID();
			Get get = new Get(Bytes.toBytes(key));
			Result rs = tb.get(get);
			if(rs==null || rs.size()==0){
				return null;
			}
			
			String pwd = Bytes.toString(rs.getValue(CF, UserTable.FIELD_PASSWORD));
			return pwd;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public long getUserCount(long domainId) throws Exception {
		Connection conn = null;
		Table tb = null;
		ResultScanner rs = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			rs = tb.getScanner(scan);
			if(rs==null){
				return 0;
			}
			long counter = 0;
			for(Result r : rs){
				counter = counter + r.size();
			}
			return counter;
		}finally{
			HBaseUtils.closeTable(tb);
			HBaseUtils.closeResult(rs);
		}
	}
	
	public List<User> getUserList(long domainId,int pageNo,int pageSize) throws Exception {
		Connection conn = null;
		Table tb = null;
		ResultScanner rs = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(userTableName));
			Scan scan = new Scan();
			Filter filete = new PageFilter(pageSize);
			rs = tb.getScanner(scan);
			Result rt =null;
			byte[] value =null;
			List<User> userList =new ArrayList<User>();
			long startIndex = pageSize * pageNo;
			long endIndex = startIndex+pageSize;
			long index=0;
			User user =null;
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
				value = rt.getValue(CF, UserTable.FIELD_NAME);
				if(value==null){
					continue;
				}
				user = new User();
				user.setName(Bytes.toString(value));
				value = rt.getValue(CF, UserTable.FIELD_DOMAIN);
				if(value!=null && value.length>0){
					user.setDomain(new MQDomain(Bytes.toLong(value)));
				}
		
				if(domainId!=user.getDomain().getId()){
					continue;
				}
				
				user.setType(Bytes.toString(rt.getValue(CF, UserTable.FIELD_TYPE)));
				user.setStatus(Bytes.toString(rt.getValue(CF, UserTable.FIELD_STATUS)));
				user.setRemark(Bytes.toString(rt.getValue(CF, UserTable.FIELD_REMARK)));
				
				value = rt.getValue(CF, UserTable.FIELD_CREATED_TIME);
				if(value!=null && value.length>0){
					user.setCreatedTime(new Date(Bytes.toLong(value)));
				}
				value = rt.getValue(CF, UserTable.FIELD_UPDATED_TIME);
				if(value!=null && value.length>0){
					user.setUpdatedTime(new Date(Bytes.toLong(value)));
				}
				userList.add(user);
				index++;
			}
			return userList;
		}finally{
			HBaseUtils.closeTable(tb);
			HBaseUtils.closeResult(rs);
		}
		
	}
}
