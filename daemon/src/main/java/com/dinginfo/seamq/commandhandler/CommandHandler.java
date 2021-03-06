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

package com.dinginfo.seamq.commandhandler;

import com.dinginfo.seamq.Command;
import com.dinginfo.seamq.MessageRequest;
import com.dinginfo.seamq.MessageResponse;



/** 
 * @author David Ding
 *
 */

public interface CommandHandler extends Command{
	public void doCommand(MessageRequest request, MessageResponse response);

}
