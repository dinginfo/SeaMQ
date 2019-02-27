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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dinginfo.seamq.entity.NodeInfo;


public final class KetamaNodeLocator {
	
	private TreeMap<Long, String> ketamaNodes;
	
	private int vnodeNum;
	
   
    
    public  KetamaNodeLocator(List<String> nodes, int vnodeNum){
    	this.vnodeNum = vnodeNum;
    	this.ketamaNodes =new TreeMap<Long, String>();
        byte[] digest=null;
        long m;
		for (String node : nodes) {
			for (int i = 0,n=vnodeNum/4; i < n; i++) {
				digest = computeMd5(node + i);
				for(int h = 0; h < 4; h++) {
					m = doHash(digest, h);
					//System.out.println(m);
					ketamaNodes.put(m, node);
				}
			}
		}
    }
    
    public void rebuildKetamaNodeMap(List<String> nodes){
    	ketamaNodes.clear();
        byte[] digest=null;
        long m;
		for (String node : nodes) {
			for (int i = 0,n=vnodeNum/4; i < n; i++) {
				digest = computeMd5(node + i);
				for(int h = 0; h < 4; h++) {
					m = doHash(digest, h);
					//System.out.println(m);
					ketamaNodes.put(m, node);
				}
			}
		}
    }


	public String getNodeInfo(final String k) {
		//byte[] digest = hashAlg.computeMd5(k);
		byte[] digest = computeMd5(k);
		String rv=getNodeForKey(doHash(digest, 0));
		return rv;
	}
	
	public List<String> getNodeList(final String key,int n){
		byte[] digest = computeMd5(key);
		
		return getNodeForKey(doHash(digest, 0),n);
	}

	private String getNodeForKey(long hash) {
		final String rv;
		Long key = hash;
		if(ketamaNodes.isEmpty()){
			return null;
		}
		if(!ketamaNodes.containsKey(key)) {
			
			SortedMap<Long, String> tailMap=ketamaNodes.tailMap(key);
			if(tailMap.isEmpty()) {
				key=ketamaNodes.firstKey();
			} else {
				key=tailMap.firstKey();
			}
			//For JDK1.6 version
//			key = ketamaNodes.ceilingKey(key);
//			if (key == null) {
//				key = ketamaNodes.firstKey();
//			}
		}
		
		
		rv=ketamaNodes.get(key);
		return rv;
	}
	
	private List<String> getNodeForKey(long hash,int n) {
		if(n==0){
			return null;
		}
		List<String> resultList=new ArrayList<String>();
		Map<String,String> map=new HashMap<String, String>();
		int index=0;
		Long key = hash;
		
		SortedMap<Long, String> tailMap=ketamaNodes.tailMap(key,true);
		if(tailMap.isEmpty()) {
			for(String nd : ketamaNodes.values()){
				if(!map.containsKey(nd)){
					resultList.add(nd);
					map.put(nd, nd);
					index++;
				}
				if(index==n){
					break;
				}
			}
			return resultList;
		} 
		
		for(String nd: tailMap.values()){
			if(!map.containsKey(nd)){
				resultList.add(nd);
				map.put(nd, nd);
				index++;
			}
			if(index==n){
				break;
			}
		}
		
		if(index==n){
			return resultList;
		}
		
		for(String nd: ketamaNodes.values()){
			if(!map.containsKey(nd)){
				resultList.add(nd);
				map.put(nd, nd);
				index++;
			}
			if(index==n){
				break;
			}
		}
		
		return resultList;
	}
	
	
	private long doHash(byte[] digest, int nTime) {
		long rv = ((long) (digest[3+nTime*4] & 0xFF) << 24)
				| ((long) (digest[2+nTime*4] & 0xFF) << 16)
				| ((long) (digest[1+nTime*4] & 0xFF) << 8)
				| (digest[0+nTime*4] & 0xFF);
		
		return rv & 0xffffffffL; /* Truncate to 32-bits */
	}
	
	
	private byte[] computeMd5(String k) {
		return k.getBytes();
		/*md5.reset();
		byte[] keyBytes = null;
		try {
			keyBytes = k.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unknown string :" + k, e);
		}
		
		md5.update(keyBytes);
		return md5.digest();*/
	}
}
