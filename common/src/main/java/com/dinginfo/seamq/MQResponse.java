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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MQResponse extends MQBasic{
	protected String status;
	
	protected String exception;
	
	public MQResponse(){
		this.attributeMap = new HashMap<String, DataField>();
		this.fieldMap = new HashMap<String, DataField>();
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	
	public String getException() {
		return exception;
	}

	public void setException(String exception) {
		this.exception = exception;
	}

	public Map<String, DataField> getAttributeMap() {
		return attributeMap;
	}

	public void setAttributeMap(Map<String, DataField> attributeMap) {
		this.attributeMap = attributeMap;
	}

}
