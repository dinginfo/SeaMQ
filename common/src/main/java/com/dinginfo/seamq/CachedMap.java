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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CachedMap<K, V> {
	private Cache<K, V> mycache;


	public CachedMap(int maxItemSize) {
		mycache = Caffeine.newBuilder().maximumSize(maxItemSize).build();
	}
	
	public CachedMap(int maxItemSize,int expireSecondTime) {
		if(expireSecondTime>0) {
			mycache = Caffeine.newBuilder().maximumSize(maxItemSize).expireAfterAccess(expireSecondTime, TimeUnit.SECONDS).build();
		}else {
			mycache = Caffeine.newBuilder().maximumSize(maxItemSize).build();
		}
	}

	public void put(K key, V value) {
		mycache.put(key, value);
	}

	public V get(K key) {
		return mycache.getIfPresent(key);
	}

	public void remove(K key) {
		mycache.invalidate(key);
	}

	public void clear(){
		mycache.invalidateAll();
		mycache.cleanUp();
		
	}
	
	public void shutdown(){
		mycache.cleanUp();
	}
	
	public Map<K,V> getAll(){
		mycache.cleanUp();
		return mycache.asMap();
	}
}
