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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiMap<T> {
	private int bucketSize =0;
	private Map<String,Map<String,T>> map;
	
	public MultiMap(int bucketSize){
		this.bucketSize = bucketSize;
		map = new HashMap<String,Map<String,T>>();
		Map<String,T> objMap =null;
		for(int i=0;i<bucketSize;i++){
			objMap = new HashMap<String,T>();
			map.put(String.valueOf(i), objMap);
		}
	}
	
	private Map<String,T> getObjectMap(String key){
		if(key==null || key.length()==0){
			return null;
		}
		int k = key.hashCode() % bucketSize;
		if(k<0){
			k = Math.abs(k);
		}
		return map.get(String.valueOf(k));
	}
	
	public boolean containsKey(String key){
		Map<String,T> objMap = getObjectMap(key);
		if(objMap==null){
			return false;
		}else{
			return objMap.containsKey(key);
		}
		
	}
	
	public void put(String key,T obj){
		Map<String,T> objMap = getObjectMap(key);
		if(objMap!=null){
			objMap.put(key, obj);
		}
	}
	
	public T get(String key){
		Map<String,T> objMap = getObjectMap(key);
		if(objMap!=null){
			return objMap.get(key);
		}else{
			return null;
		}
	}
	
	public Object remove(String key){
		Map<String,T> objMap = getObjectMap(key);
		if(objMap!=null){
			return objMap.remove(key);
		}else{
			return null;
		}
	}
	
	public void clear(){
		Map<String,T> objMap =null;
		for(int i=0;i<bucketSize;i++){
			objMap = map.get(String.valueOf(i));
			if(objMap!=null){
				objMap.clear();
			}
		}
		map.clear();
	}
	
	public Map<String,T> getMap(String mapKey){
		return map.get(mapKey);
	}
	
	public List<T> getObjectList(){
		List<T> objList =new ArrayList<T>();
		for(Map<String,T> objMap : map.values()){
			for(T obj : objMap.values()){
				objList.add(obj);
			}
		}
		return objList;
	}
	
	public int getSize(){
		int n=0;
		for(Map<String,T> objMap : map.values()){
			n = n + objMap.size();
		}
		return n;
	}
}
