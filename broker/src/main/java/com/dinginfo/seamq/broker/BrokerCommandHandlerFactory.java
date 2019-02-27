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
package com.dinginfo.seamq.broker;

import java.util.HashMap;
import java.util.Map;

import com.dinginfo.seamq.commandhandler.CommandHandler;
import com.dinginfo.seamq.commandhandler.LoginHandler;
import com.dinginfo.seamq.commandhandler.LogoutHandler;
import com.dinginfo.seamq.commandhandler.MessageHandler;
import com.dinginfo.seamq.commandhandler.RegisterHandler;
import com.dinginfo.seamq.commandhandler.SessionHandler;
import com.dinginfo.seamq.commandhandler.TopicHandler;

/** 
 * @author David Ding
 *
 */
public class BrokerCommandHandlerFactory {
	private static Map<String,String> directAccessMap = new HashMap<String,String>();
	
	private static Map<String, CommandHandler> handlerMap = new HashMap<String, CommandHandler>();
	static {
		
		handlerMap.put(CommandHandler.COMMAND_GET, new MessageHandler(MessageHandler.ACTION_GET));
		handlerMap.put(CommandHandler.COMMAND_DEL, new MessageHandler(MessageHandler.ACTION_DELETE));
		handlerMap.put(CommandHandler.COMMAND_PUT, new MessageHandler(MessageHandler.ACTION_PUT));
		
		handlerMap.put(CommandHandler.COMMAND_TOPIC_GET, new TopicHandler(TopicHandler.ACTION_GET));
		handlerMap.put(CommandHandler.COMMAND_TOPIC_DISABLE_QUEUE_STATUS, new TopicHandler(TopicHandler.ACTION_DISABLE_QUEUE_STATUS));
		
		handlerMap.put(CommandHandler.COMMAND_LOGIN, new LoginHandler());
		handlerMap.put(CommandHandler.COMMAND_LOGOUT, new LogoutHandler());
		handlerMap.put(CommandHandler.COMMAND_UPDATE_SESSION, new SessionHandler());
		handlerMap.put(CommandHandler.COMMAND_REGISTER, new RegisterHandler());
		
		
		//direct Access Map
		directAccessMap.put(CommandHandler.COMMAND_TOPIC_GET, "");
		directAccessMap.put(CommandHandler.COMMAND_LOGIN, "");
		directAccessMap.put(CommandHandler.COMMAND_LOGOUT, "");
		directAccessMap.put(CommandHandler.COMMAND_UPDATE_SESSION, "");
	}
	
	public static CommandHandler geCommandHandler(String command){
		return handlerMap.get(command);
	}
	
	public static boolean isDriectAccess(String command){
		return directAccessMap.containsKey(command);
	}

}
