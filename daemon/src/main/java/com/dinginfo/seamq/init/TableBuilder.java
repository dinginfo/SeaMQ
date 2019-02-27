package com.dinginfo.seamq.init;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import com.dinginfo.seamq.common.DBUtil;
import com.dinginfo.seamq.common.MyBean;

public class TableBuilder {
	public void createMessage(int n)throws Exception{
		DataSource ds = MyBean.getBean("dataSource", DataSource.class);
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = ds.getConnection();
			stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			String sql = null;
			for(int i=0;i<n;i++) {
				sb = new StringBuilder();
				sb.append("CREATE TABLE t_message_");
				sb.append(i);
				sb.append("(");
				sb.append("CONSTRAINT pk_t_message_");
				sb.append(i);
				sb.append(" PRIMARY KEY (ms_id)");
				sb.append(")INHERITS (t_message)WITH (OIDS=FALSE);");
				sql = sb.toString();
				stmt.execute(sql);
			}
			
		}finally {
			DBUtil.closeConnection(conn,stmt,null);
		}
	}

	public static void main(String[] args) {
		TableBuilder tb = new TableBuilder();
		try {
			tb.createMessage(100);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
