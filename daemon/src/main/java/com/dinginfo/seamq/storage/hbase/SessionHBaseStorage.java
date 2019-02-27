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

import java.util.Date;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.dinginfo.seamq.entity.MQDomain;
import com.dinginfo.seamq.entity.MQSession;
import com.dinginfo.seamq.entity.User;
import com.dinginfo.seamq.storage.hbase.mapping.SessionTable;


public class SessionHBaseStorage{
	public static final String BEAN_NAME="sessionStorage";

	private final byte[] CF = Bytes.toBytes("cf");

	private HBaseDataSource dataSource;
	
	private String sessionTableName = null;

	
	public SessionHBaseStorage(String namespace){
		sessionTableName = HBaseUtils.buildTalbeName(namespace, SessionTable.TABLE_NAME);
	}
	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}

	
	public MQSession create(MQSession session) throws Exception {
		Connection conn = dataSource.getConnection();
		Table tb = conn.getTable(TableName.valueOf(sessionTableName));
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(sessionTableName));
			Put put = new Put(Bytes.toBytes(session.getId()));
			put.addColumn(CF, SessionTable.FIELD_SESSION_ID, Bytes.toBytes(session.getId()));
			if(session.getUser()!=null){
				User user = session.getUser();
				put.addColumn(CF, SessionTable.FIELD_USER_ID, Bytes.toBytes(user.getName()));
				put.addColumn(CF, SessionTable.FIELD_DOMAIN_ID, Bytes.toBytes(user.getDomain().getId()));
				if(user.getType()!=null){
					put.addColumn(CF, SessionTable.FIELD_USER_TYPE, Bytes.toBytes(user.getType()));
				}
			}
			
			if (session.getUpdatedTime() != null) {
				put.addColumn(CF, SessionTable.FIELD_UPDATE_TIME,
						Bytes.toBytes(session.getUpdatedTime().getTime()));
			}
			tb.put(put);
			return session;
		}finally{
			HBaseUtils.closeTable(tb);
		}
	}

	
	public int update(MQSession session) throws Exception {

		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(sessionTableName));
			Put put = new Put(Bytes.toBytes(session.getId()));
			put.addColumn(CF, SessionTable.FIELD_UPDATE_TIME,
					Bytes.toBytes(session.getUpdatedTime().getTime()));
			tb.put(put);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public int deleteByPK(String sessionId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(sessionTableName));
			Delete del = new Delete(Bytes.toBytes(sessionId));
			tb.delete(del);
			return 1;
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}

	
	public MQSession getByPK(String sessionId) throws Exception {
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(sessionTableName));
			MQSession session = null;
			Get get = new Get(Bytes.toBytes(sessionId));
			Result rs = tb.get(get);
			if(rs==null || rs.size()==0){
				return null;
			}
			byte[] value = null;
			session = new MQSession();
			session.setId(Bytes.toString(rs.getValue(CF, SessionTable.FIELD_SESSION_ID)));
			User user = new User();
			user.setName(Bytes.toString(rs.getValue(CF, SessionTable.FIELD_USER_ID)));
			
			value = rs.getValue(CF, SessionTable.FIELD_USER_TYPE);
			if (value != null) {
				user.setType(Bytes.toString(value));;
			}
			value = rs.getValue(CF, SessionTable.FIELD_DOMAIN_ID);
			if(value!=null){
				user.setDomain(new MQDomain(Bytes.toLong(value)));
			}
			session.setUser(user);
			value = rs.getValue(CF, SessionTable.FIELD_UPDATE_TIME);
			if (value != null) {
				session.setUpdatedTime(new Date(Bytes.toLong(value)));
			}
		
			return session;
			
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
	}
	
	
	
	
	public int clearOldSession(long timeout) throws Exception {

		Connection conn = null;
		Table tb = null;
		
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(sessionTableName));
			Scan scan = new Scan();
			ResultScanner rs = tb.getScanner(scan);
			Result rt =null;
			byte[] rowKey =null;
			Delete del = null;
			int result=0;
			byte[] value =null;
			long updatedTime =0;
			long currentTime = System.currentTimeMillis();
			while(true){
				rt = rs.next();
				if(rt==null){
					break;
				}
				value = rt.getValue(CF, SessionTable.FIELD_UPDATE_TIME);
				if(value==null || value.length==0){
					continue;
				}
				updatedTime = Bytes.toLong(value);
				if((currentTime - updatedTime)<timeout){
					continue;
				}
				rowKey = rt.getRow();
				del = new Delete(rowKey);
				tb.delete(del);
				result++;
			}
			return result;
		}finally{
			
			HBaseUtils.closeTable(tb);
		}
		
	}


}
