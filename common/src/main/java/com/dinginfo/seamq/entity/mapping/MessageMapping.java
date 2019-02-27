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

public interface MessageMapping {
	public static final String RESULT="result";
	
	public static final String FIELD_VERSION ="ver";
	
	public static final String FIELD_MESSAGE_ID ="msgid";
	
	public static final String FIELD_DOMAIN_ID="domainid";
	
	public static final String FIELD_TOPIC_ID="topicid";
	
	public static final String FIELD_TOPIC_NAME="topicname";
	
	public static final String FIELD_QUEUE_ID="queueid";
	
	public static final String FIELD_QUEUE="queue";
	
	public static final String FIELD_CONSUMER_GROUP = "gname";
	
	public static final String FIELD_ARRAY_SIZE="arraysize";
	
	public static final String FIELD_OFFSET="offset";
	
	

}
