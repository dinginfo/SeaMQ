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
package com.dinginfo.seamq.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dinginfo.seamq.service.SessionService;

public class ClearSessionTask implements Runnable {
	private static final Logger logger = LogManager.getLogger(ClearSessionTask.class);
	
	private SessionService service;

	private long timeout;

	public ClearSessionTask(SessionService sessionService, long sessionTimeout) {
		this.service = sessionService;
		this.timeout = sessionTimeout;
	}

	@Override
	public void run() {
		try {
			logger.info("begin of clear session");
			int n =service.clearOldSession(timeout);
			logger.info("end of clear session,old session num:"+n);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

}
