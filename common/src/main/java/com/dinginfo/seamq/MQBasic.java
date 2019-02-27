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

public abstract class MQBasic {
	protected Map<String, DataField> fieldMap;

	protected Map<String, DataField> attributeMap;
	
	protected String serviceName;

	protected String version;

	protected String requestId;
	
	protected byte[] data;
	
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Map<String, DataField> getFieldMap() {
		return fieldMap;
	}

	public void setFieldMap(Map<String, DataField> fieldMap) {
		this.fieldMap = fieldMap;
	}

	public void setFields(List<DataField> fields) {
		if(fields==null || fields.size()==0){
			return;
		}
		this.fieldMap = new HashMap<String, DataField>();
		for(DataField field : fields){
			fieldMap.put(field.getKey(), field);
		}
	}

	public Map<String, DataField> getAttributeMap() {
		return attributeMap;
	}

	public void setAttributeMap(Map<String, DataField> attributeMap) {
		this.attributeMap = attributeMap;
	}
	
	
	public DataField getAttribute(String key){
		if(attributeMap!=null){
			return attributeMap.get(key);
		}else{
			return null;
		}
	}
	
	public DataField getField(String key){
		if(fieldMap!=null){
			return fieldMap.get(key);
		}else{
			return null;
		}
	}
	
	
	public String getStringAttribute(String key){
		return getString(attributeMap, key);
	}
	
	public String getStringField(String key){
		return getString(fieldMap, key);
	}
	
	public int getIntAttribute(String key){
		return getInt(attributeMap, key);
	}
	
	public int getIntField(String key){
		return getInt(fieldMap, key);
	}
	
	public long getLongAttribute(String key){
		return getLong(attributeMap, key);
	}
	
	public long getLongField(String key){
		return getLong(fieldMap, key);
	}
	
	public short getShortAttribute(String key){
		return getShort(attributeMap, key);
	}
	
	public short getShortField(String key){
		return getShort(fieldMap, key);
	}
	
	public float getFloatAttribute(String key){
		return getFloat(attributeMap, key);
	}
	public float getFloatField(String key){
		return getFloat(fieldMap, key);
	}
	
	public double getDoubleAttribute(String key){
		return getDouble(attributeMap, key);
	}
	
	public double getDoubleField(String key){
		return getDouble(fieldMap, key);
	}
	
	public boolean isBooleanAttribute(String key){
		return isBoolean(attributeMap, key);
	}
	
	public boolean isBooleanField(String key){
		return isBoolean(fieldMap, key);
	}
	
	public byte[] getByteAttribute(String key){
		return getByte(attributeMap, key);
	}
	
	public byte[] getByteField(String key){
		return getByte(fieldMap, key);
	}
	
	public void setAttribute(String key,String value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,int value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,long value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,float value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,double value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,boolean value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,short value){
		setDataField(attributeMap, key, value);
	}
	
	public void setAttribute(String key,byte[] value){
		setDataField(attributeMap, key, value);
	}
	
	public void setField(String key,String value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,int value){
		setDataField(fieldMap, key, value);
	}
	
	
	public void setField(String key,long value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,short value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,float value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,double value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,boolean value){
		setDataField(fieldMap, key, value);
	}
	
	public void setField(String key,byte[] value){
		setDataField(fieldMap, key, value);
	}
	private String getString(Map<String, DataField> map,String key){
		if(map==null){
			return null;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getString();
		}else{
			return null;
		}
	}
	
	private int getInt(Map<String, DataField> map,String key){
		if(map==null){
			return 0;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getInt();
		}else{
			return 0;
		}
	}
	
	private long getLong(Map<String, DataField> map,String key){
		if(map==null){
			return 0L;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getLong();
		}else{
			return 0L;
		}
	}
	
	private short getShort(Map<String, DataField> map,String key){
		if(map==null){
			return 0;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getShort();
		}else{
			return 0;
		}
	}
	
	private float getFloat(Map<String, DataField> map,String key){
		if(map==null){
			return 0;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getFloat();
		}else{
			return 0;
		}
	}
	
	private double getDouble(Map<String, DataField> map,String key){
		if(map==null){
			return 0;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getDouble();
		}else{
			return 0;
		}
	}
	
	private boolean isBoolean(Map<String, DataField> map,String key){
		if(map==null){
			return false;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.isBoolean();
		}else{
			return false;
		}
	}
	
	private byte[] getByte(Map<String, DataField> map,String key){
		if(map==null){
			return null;
		}
		DataField attribute = map.get(key);
		if(attribute!=null){
			return attribute.getData();
		}else{
			return null;
		}
	}
	
	private void setDataField(Map<String, DataField> map,String key,String value){
		if(key==null || value==null){
			return ;
		}
		DataField bean = new DataField(key, value);
		map.put(key, bean);
	}
	
	private void setDataField(Map<String, DataField> map,String key,int value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setInt(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,long value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setLong(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,short value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setShort(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,float value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setFloat(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,double value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setDouble(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,boolean value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key);
		field.setBoolean(value);
		map.put(key, field);
	}
	
	private void setDataField(Map<String, DataField> map,String key,byte[] value){
		if(key==null){
			return ;
		}
		DataField field = new DataField(key,value);
		map.put(key, field);
	}
}
