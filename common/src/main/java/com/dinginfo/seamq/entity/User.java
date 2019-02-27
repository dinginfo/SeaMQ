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

package com.dinginfo.seamq.entity;

import java.io.Serializable;
import java.util.Date;

import com.dinginfo.seamq.MQConstant;
import com.dinginfo.seamq.common.StringUtil;

public class User implements Serializable {
	public static final String TYPE_ADMIN = "ADMIN";
	
	private MQDomain domain;

	private String name;

	private String pwd;

	private Date createdTime;

	private Date updatedTime;

	private String status;

	private String type;
	
	private String groupName;

	private String remark;


	public User() {
	}

	public User(String username) {
		this.name = username;
	}

	public String getName() {
		return name;
	}

	public void setName(String id) {
		this.name = id;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public Date getUpdatedTime() {
		return updatedTime;
	}

	public void setUpdatedTime(Date updatedTime) {
		this.updatedTime = updatedTime;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public MQDomain getDomain() {
		return domain;
	}

	public void setDomain(MQDomain domain) {
		this.domain = domain;
	}

	public String buildUserObjectID() throws Exception {
		if (name == null) {
			throw new Exception("user id is null");
		}
		if(!TYPE_ADMIN.equals(type)){
			if(domain==null){
				throw new Exception("user domain is null");
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(domain.getId()));
		sb.append(MQConstant.SEPARATOR);
		sb.append(name);
		return StringUtil.generateMD5String(sb.toString());
	}

}
