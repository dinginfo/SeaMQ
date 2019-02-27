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

package com.dinginfo.seamq.entity.mapping;

public interface TopicMapping {
	public static final String REG_TYPE_TOPIC = "TOPIC";
	
	public static final String REG_TYPE_QUEUE = "QUEUE";
	
	public static final String REG_TYPE = "regtype";
	
	public static final String RESULT="result";
	
	public static final String FIELD_ID="id";
	
	public static final String FIELD_NAME="name";
	
	public static final String FIELD_QUEUE_NUM="qnum";
	
	public static final String FIELD_REMARK="remark";
	
	public static final String FIELD_TYPE="type";
	
	public static final String FIELD_STSTUS="status";
	
	public static final String FIELD_USERID="userId";
	
	public static final String FIELD_GRANTTYPE="gtype";
	
	public static final String FIELD_CONSUMER_GROUP_NAME ="gname";
	
	public static final String FIELD_PAGE_SIZE="pagesize";
	
	public static final String FIELD_PAGE_NO="pageno";
}
