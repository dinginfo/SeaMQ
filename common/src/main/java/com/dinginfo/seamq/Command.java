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

package com.dinginfo.seamq;

/** 
 * @author David Ding
 *
 */
public interface Command {
	//message command
	public final static String COMMAND_PUT="put";  
	public final static String COMMAND_GET="get";
	public final static String COMMAND_DEL="del";
	//response command
	public final static String COMMAND_RESPONSE="resp";
	
	//notification command
	public final static String COMMAND_NOTIFY="noti";
	
	//regiter
	public final static String COMMAND_REGISTER="regist";
	
	//login and logout
	public final static String COMMAND_LOGIN="login";
	public final static String COMMAND_LOGOUT="logout";

	//user command
	public final static String COMMAND_USER_GET="100";
	public final static String COMMAND_USER_CREATE="101";
	public final static String COMMAND_USER_UPDATE="102";
	public final static String COMMAND_USER_UPDATE_PASSWD="103";
	public final static String COMMAND_USER_DELETE="104";
	public final static String COMMAND_USER_COUNT="105";
	public final static String COMMAND_USER_LIST="106";
	
	//topic command
	public final static String COMMAND_TOPIC_GET="201";
	public final static String COMMAND_TOPIC_CREATE="202";
	public final static String COMMAND_TOPIC_UPDATE="203";
	public final static String COMMAND_TOPIC_DROP="204";
	
	public final static String COMMAND_TOPIC_DISABLE="207";
	public final static String COMMAND_TOPIC_ENABLE="208";
	public final static String COMMAND_TOPIC_DISABLE_QUEUE="209";
	public final static String COMMAND_TOPIC_DISABLE_QUEUE_STATUS="210";
	public final static String COMMAND_TOPIC_ENABLE_QUEUE="211";
	public final static String COMMAND_TOPIC_INCREASE_QUEUE="212";
	public final static String COMMAND_TOPIC_REDUCE_QUEUE="213";
	public final static String COMMAND_TOPIC_COUNT="214";
	public final static String COMMAND_TOPIC_LIST="215";
	public final static String COMMAND_TOPIC_USER_LIST="216";
	public final static String COMMAND_TOPIC_GRANT_USER_TO_PRODUCER="217";
	public final static String COMMAND_TOPIC_REMOVE_PRODUCER_USER="218";
	public final static String COMMAND_TOPIC_GET_PRODUCER_USER_LIST="219";
	public final static String COMMAND_TOPIC_CREATE_CONSUMER_GROUP="220";
	public final static String COMMAND_TOPIC_DROP_CONSUMER_GROUP="221";
	public final static String COMMAND_TOPIC_GET_CONSUMER_GROUP_LIST="222";
	public final static String COMMAND_TOPIC_GRANT_USER_TO_CONSUMER_GROUP="223";
	public final static String COMMAND_TOPIC_REMOVE_CONSUMER_USER="224";
	public final static String COMMAND_TOPIC_GET_CONSUMER_USER_LIST="225";
	


	
	//session command
	public final static String COMMAND_UPDATE_SESSION="301";
	
	//queue command
	public final static String COMMAND_QUEUE_GET="401";
	public final static String COMMAND_QUEUE_CREATE="402";
	public final static String COMMAND_QUEUE_GET_LOCATION="403";
	public final static String COMMAND_QUEUE_SET_LOCATION="404";
	public final static String COMMAND_QUEUE_LIST="405";

	
	
}
