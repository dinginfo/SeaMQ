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

import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.config.HBaseApplicationContext;
import com.dinginfo.seamq.config.JDBCApplicationContext;


/**
 * @author ding
 * 
 * TODO
 */
public class MyBean {
	private static ApplicationContext appContext;
	
	public static void start(){
		if(appContext==null){
			createStrpingContext();
		}
	}

	public static <T> T getBean(String beanId,Class<T> classType) {
		if(appContext==null){
			createStrpingContext();
		}
		return appContext.getBean(beanId, classType);
	}
	
	private synchronized static void createStrpingContext(){
		if(appContext!=null){
			return ;
		}
		ServerConfig config = new ServerConfig();
		String storageType = config.getStorageType();
		if(ServerConfig.STORAGE_JDBC.equalsIgnoreCase(storageType)){
			appContext = new JDBCApplicationContext();
		}else if(ServerConfig.STORAGE_HBASE.equalsIgnoreCase(storageType)) {
			appContext = new HBaseApplicationContext();
		}
		
	}
}
