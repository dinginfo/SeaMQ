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

package com.dinginfo.seamq.storage.hbase.mapping;

import org.apache.hadoop.hbase.util.Bytes;

public interface QueueTable {
	public static final String TABLE_NAME = "queue";

	public static final byte[] FIELD_QUEUE_ID = Bytes.toBytes(1);

	public static final byte[] FIELD_TOPIC_ID = Bytes.toBytes(2);

	public static final byte[] FIELD_QUEUE = Bytes.toBytes(3);
	
	public static final byte[] FIELD_DOMAIN_ID = Bytes.toBytes(4);
	
	public static final byte[] FIELD_STATUS= Bytes.toBytes(5);

	public static final byte[] FIELD_CREATED_TIME = Bytes.toBytes(6);

	public static final byte[] FIELD_IN_OFFSET = Bytes.toBytes(7);
}
