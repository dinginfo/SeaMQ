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

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.dinginfo.seamq.common.MyBean;

public class TableManager {
	private HBaseDataSource dataSource;
	
	public void setDataSource(HBaseDataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void clearTable(String tableName)throws Exception{
		Connection conn = null;
		Table tb = null;
		try{
			conn = dataSource.getConnection();
			tb = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner rs = tb.getScanner(scan);
			Result rt =null;
			List<Cell> cellList=null;
			Cell cell =null;
			byte[] rowKey =null;
			Delete del = null;
			StringBuilder sb =null;
			int i=0;
			while(true){
				rt = rs.next();
				if(rt==null){
					break;
				}
				
				rowKey = rt.getRow();
				del = new Delete(rowKey);
				tb.delete(del);
				i++;
				if((i % 100)==0){
					sb = new StringBuilder(50);
					sb.append(i);
					sb.append(":");
					sb.append(Bytes.toString(rowKey));
					System.out.println(sb.toString());	
				}
				
			}
		}finally{
			HBaseUtils.closeTable(tb);
		}
		
		
	}

	public static void main(String[] args) {
		TableManager manager = new TableManager();
		HBaseDataSource dataSource = MyBean.getBean("dataSource", HBaseDataSource.class);
		manager.setDataSource(dataSource);
		try {
			manager.clearTable("");
		} catch (Exception e) {
			
			e.printStackTrace();
		}

	}

}
