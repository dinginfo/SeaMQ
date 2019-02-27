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

package com.dinginfo.seamq.master;

import java.util.HashMap;
import java.util.Map;

import com.dinginfo.seamq.commandhandler.CommandHandler;
import com.dinginfo.seamq.commandhandler.LoginHandler;
import com.dinginfo.seamq.commandhandler.LogoutHandler;
import com.dinginfo.seamq.commandhandler.QueueHandler;
import com.dinginfo.seamq.commandhandler.SessionHandler;
import com.dinginfo.seamq.commandhandler.TopicHandler;
import com.dinginfo.seamq.commandhandler.UserHandler;

/** 
 * @author David Ding
 *
 */
public class MasterCommandHandlerFactory {
	private static Map<String, CommandHandler> handlerMap = new HashMap<String, CommandHandler>();
	static {
		handlerMap.put(CommandHandler.COMMAND_USER_GET, new UserHandler(UserHandler.ACTION_GET));
		handlerMap.put(CommandHandler.COMMAND_USER_CREATE, new UserHandler(UserHandler.ACTION_CREATE));
		handlerMap.put(CommandHandler.COMMAND_USER_UPDATE, new UserHandler(UserHandler.ACTION_UPDATE));
		handlerMap.put(CommandHandler.COMMAND_USER_DELETE, new UserHandler(UserHandler.ACTION_DELETE));
		handlerMap.put(CommandHandler.COMMAND_USER_UPDATE_PASSWD, new UserHandler(UserHandler.ACTION_UPDATE_PASSWD));
		handlerMap.put(CommandHandler.COMMAND_USER_COUNT, new UserHandler(UserHandler.ACTION_COUNT));
		handlerMap.put(CommandHandler.COMMAND_USER_LIST, new UserHandler(UserHandler.ACTION_LIST));
		//topic
		handlerMap.put(CommandHandler.COMMAND_TOPIC_CREATE, new TopicHandler(TopicHandler.ACTION_CREATE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_UPDATE, new TopicHandler(TopicHandler.ACTION_UPDATE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_DISABLE, new TopicHandler(TopicHandler.ACTION_DISABLE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_DISABLE_QUEUE, new TopicHandler(TopicHandler.ACTION_DISABLE_QUEUE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_ENABLE, new TopicHandler(TopicHandler.ACTION_ENABLE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_ENABLE_QUEUE, new TopicHandler(TopicHandler.ACTION_ENABLE_QUEUE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_DROP, new TopicHandler(TopicHandler.ACTION_DROP));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_INCREASE_QUEUE, new TopicHandler(TopicHandler.ACTION_INCREASE_QUEUE));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_REDUCE_QUEUE, new TopicHandler(TopicHandler.ACTION_REDUCE_QUEUE));
		
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GET, new TopicHandler(TopicHandler.ACTION_GET));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_COUNT, new TopicHandler(TopicHandler.ACTION_COUNT));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_LIST, new TopicHandler(TopicHandler.ACTION_LIST));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_USER_LIST, new TopicHandler(TopicHandler.ACTION_USER_LIST));
		
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GRANT_USER_TO_PRODUCER, new TopicHandler(TopicHandler.ACTION_GRANT_USER_TO_PRODUCER));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_REMOVE_PRODUCER_USER, new TopicHandler(TopicHandler.ACTION_REMOVE_PRODUCER_USER));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GET_PRODUCER_USER_LIST, new TopicHandler(TopicHandler.ACTION_GET_PRODUCER_USER_LIST));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_CREATE_CONSUMER_GROUP, new TopicHandler(TopicHandler.ACTION_CREATE_CONSUMER_GROUP));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_DROP_CONSUMER_GROUP, new TopicHandler(TopicHandler.ACTION_DROP_CONSUMER_GROUP));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GET_CONSUMER_GROUP_LIST, new TopicHandler(TopicHandler.ACTION_GET_CONSUMER_GROUP_LIST));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GRANT_USER_TO_CONSUMER_GROUP, new TopicHandler(TopicHandler.ACTION_GRANT_USER_TO_CONSUMER_GROUP));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_REMOVE_CONSUMER_USER, new TopicHandler(TopicHandler.ACTION_REMOVE_CONSUMER_USER));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GET_CONSUMER_USER_LIST, new TopicHandler(TopicHandler.ACTION_GET_CONSUMER_USER_LIST));
		
		
		//
		
		handlerMap.put(CommandHandler.COMMAND_QUEUE_GET, new QueueHandler(QueueHandler.ACTION_GET));
		handlerMap.put(CommandHandler.COMMAND_QUEUE_LIST, new QueueHandler(QueueHandler.ACTION_GET_LIST));
		handlerMap.put(CommandHandler.COMMAND_LOGIN, new LoginHandler());
		handlerMap.put(CommandHandler.COMMAND_LOGOUT, new LogoutHandler());
		handlerMap.put(CommandHandler.COMMAND_UPDATE_SESSION, new SessionHandler());
		
	}
	
	public static CommandHandler geCommandHandler(String command){
		return handlerMap.get(command);
	}
}
