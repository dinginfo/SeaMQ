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
package com.dinginfo.seamq.storage;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.DataField;

public class AttributeUtils {
	
	public static String toJsonString(List<DataField> attList)throws Exception{
		if(attList==null || attList.size()==0){
			return null;
		}
		List<MQKeyValue> kvList = new ArrayList<MQKeyValue>();
		MQKeyValue kv = null;
		for(DataField bean : attList){
			kv = new MQKeyValue();
			kv.setK(bean.getKey());
			kv.setT(bean.getDataType());
			kv.setV(bean.getData());
			kvList.add(kv);
		}
		String jsonStirng = JSON.toJSONString(kvList);
		return jsonStirng;
	}

}
