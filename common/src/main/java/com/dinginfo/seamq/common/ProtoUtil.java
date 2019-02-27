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

import com.dinginfo.seamq.DataField;
import com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro;
import com.dinginfo.seamq.protobuf.RequestProto.RequestPro;
import com.google.protobuf.ByteString;

public class ProtoUtil {

	public static void setAttribute(RequestPro.Builder resBuilder,DataField field) {
		if(field==null){
			return ;
		}
		if(field.getKey()==null){
			return ;
		}
		DataFieldPro.Builder attBuilder = DataFieldPro.newBuilder();
		attBuilder.setKey(field.getKey());
		attBuilder.setDataType(field.getDataType());
		attBuilder.setData(ByteString.copyFrom(field.getData()));
		resBuilder.addAttributes(attBuilder.build());
	}
	
	public static void setAttribute(RequestPro.Builder resBuilder,String key,String value){
		if(value==null){
			return;
		}
		DataField field = new DataField(key, value);
		setAttribute(resBuilder, field);
	}
	
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			int value) {
		DataField field = new DataField(key);
		field.setInt(value);
		setAttribute(resBuilder, field);
	}
	
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			long value) {
		DataField field = new DataField(key);
		field.setLong(value);
		setAttribute(resBuilder, field);
	}
	
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			float value) {
		DataField field = new DataField(key);
		field.setFloat(value);
		setAttribute(resBuilder, field);
	}
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			double value) {
		DataField field = new DataField(key);
		field.setDouble(value);
		setAttribute(resBuilder, field);
	}
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			boolean value) {
		DataField field = new DataField(key);
		field.setBoolean(value);
		setAttribute(resBuilder, field);
	}
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			short value) {
		DataField field = new DataField(key);
		field.setShort(value);
		setAttribute(resBuilder, field);
	}
	
	public static void setAttribute(RequestPro.Builder resBuilder, String key,
			byte[] value) {
		if(value==null || value.length==0){
			return;
		}
		DataField field = new DataField(key,value);
		setAttribute(resBuilder, field);
	}
	
	
	//set fields data
	
	public static void setField(RequestPro.Builder resBuilder,DataField field) {
		if(field==null){
			return ;
		}
		if(field.getKey()==null){
			return ;
		}
		DataFieldPro.Builder attBuilder = DataFieldPro.newBuilder();
		attBuilder.setKey(field.getKey());
		attBuilder.setDataType(field.getDataType());
		attBuilder.setData(ByteString.copyFrom(field.getData()));
		resBuilder.addFields(attBuilder.build());
	}
	
	public static void setField(RequestPro.Builder resBuilder,String key,String value){
		if(value==null){
			return;
		}
		DataField field = new DataField(key, value);
		setField(resBuilder, field);
	}
	
	public static void setField(RequestPro.Builder resBuilder, String key,
			int value) {
		DataField field = new DataField(key);
		field.setInt(value);
		setField(resBuilder, field);
	}
	
	public static void setField(RequestPro.Builder resBuilder, String key,
			long value) {
		DataField field = new DataField(key);
		field.setLong(value);
		setField(resBuilder, field);
	}
	
	public static void setField(RequestPro.Builder resBuilder, String key,
			float value) {
		DataField field = new DataField(key);
		field.setFloat(value);
		setField(resBuilder, field);
	}
	public static void setField(RequestPro.Builder resBuilder, String key,
			double value) {
		DataField field = new DataField(key);
		field.setDouble(value);
		setField(resBuilder, field);
	}
	public static void setField(RequestPro.Builder resBuilder, String key,
			boolean value) {
		DataField field = new DataField(key);
		field.setBoolean(value);
		setField(resBuilder, field);
	}
	public static void setField(RequestPro.Builder resBuilder, String key,
			short value) {
		DataField field = new DataField(key);
		field.setShort(value);
		setField(resBuilder, field);
	}
	
	public static void setField(RequestPro.Builder resBuilder, String key,
			byte[] value) {
		if(value==null || value.length==0){
			return;
		}
		DataField field = new DataField(key,value);
		setField(resBuilder, field);
	}
	

}
