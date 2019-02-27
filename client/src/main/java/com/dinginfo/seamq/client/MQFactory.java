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

package com.dinginfo.seamq.client;

import com.dinginfo.seamq.CachedMap;
import com.dinginfo.seamq.entity.MQDomain;

public class MQFactory extends BasicFactory{	
	private static MQFactory instance;

	public static MQFactory getInstance(){
		if(instance==null){
			createMQFactory();
		}
		return instance;
	} 
	
	private static synchronized void createMQFactory(){
		if(instance!=null){
			return;
		}
		instance = new MQFactory();
	}
	
	private MQFactory(){
		init();
	}
}
