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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

public class HBaseDataSource {
	public static final String BEAN_NAME="dataSource";

	private int zkPort;

	private String zkQuorum;

	private Connection conn;


	public void setZkPort(int zkPort) {
		this.zkPort = zkPort;
	}

	public void setZkQuorum(String zkQuorum) {
		this.zkQuorum = zkQuorum;
	}

	public Connection getConnection() throws Exception {
		if (conn != null) {
			return conn;
		}else{
			createConnection();
			return conn;	
		}
	}
	
	private synchronized void createConnection()throws Exception{
		if(conn!=null){
			return;
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conn = ConnectionFactory.createConnection(conf);
	}

}
