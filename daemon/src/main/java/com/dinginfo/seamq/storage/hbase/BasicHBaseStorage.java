package com.dinginfo.seamq.storage.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.dinginfo.seamq.storage.hbase.mapping.MessageTable;

public abstract class BasicHBaseStorage {
	protected int hbaseVersion =2;
	
	public void setHbaseVersion(int hbaseVersion) {
		if(hbaseVersion>0) {
			this.hbaseVersion = hbaseVersion;	
		}
	}

	public int getHbaseVersion() {
		return hbaseVersion;
	}
}
