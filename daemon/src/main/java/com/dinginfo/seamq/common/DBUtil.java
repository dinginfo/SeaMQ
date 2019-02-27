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
package com.dinginfo.seamq.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DBUtil {
	public static Connection getConnection(String driverName, String url,
			String userName, String pass) throws Exception {
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(url, userName, pass);
			return con;

		} catch (Exception ex) {
			ex.printStackTrace();
			//log("Error:" + ex.getMessage());
			return null;
		}

	}
	
	public static List<String> getAllTableName(Connection conn){
		List<String> list=new ArrayList<String>();
		ResultSet rs=null;
		try {
			rs = conn.getMetaData().getTables(null, null, "%",new String[]{"TABLE"});
			while (rs.next()){
				list.add(rs.getString(3));		
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static void connectionRollback(Connection conn) {
		try {
			if (conn != null)
				conn.rollback();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeConnection(Connection conn, Statement stmt,
			ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();

		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeStatement(Statement stmt) {
		try {
			if (stmt != null)
				stmt.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeResultSet(ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeConnection(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}

	}
}
