package com.dinginfo.seamq.storage.hbase.mapping;

import org.apache.hadoop.hbase.util.Bytes;

public interface OutPositionTable {
	public static final String TABLE_NAME = "out";
	
	public static final byte[] FIELD_OUT_OFFSET = Bytes.toBytes(1);
	
	public static final byte[] FIELD_QUEUE_ID = Bytes.toBytes(2);
	
	public static final byte[] FIELD_GROUP_NAME = Bytes.toBytes(3);

}
