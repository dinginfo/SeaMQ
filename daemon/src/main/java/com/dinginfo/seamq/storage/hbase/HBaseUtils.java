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

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

public class HBaseUtils {
	public static void closeTable(Table tb) {
		if (tb != null) {
			try {
				tb.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void closeResult(ResultScanner rs){
		if(rs!=null){
			rs.close();
		}
	}

	public static String buildTalbeName(String namespace, String tableName) {
		StringBuilder sb = new StringBuilder(50);
		sb.append(namespace);
		sb.append(":");
		sb.append(tableName);
		return sb.toString();
	}

}
