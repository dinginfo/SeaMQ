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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

public class MQKeyValue implements Serializable{
	private String k;
	
	private short t;
	
	private byte[] v;

	public String getK() {
		return k;
	}

	public void setK(String k) {
		this.k = k;
	}

	public short getT() {
		return t;
	}

	public void setT(short t) {
		this.t = t;
	}

	public byte[] getV() {
		return v;
	}

	public void setV(byte[] v) {
		this.v = v;
	}

	
}
