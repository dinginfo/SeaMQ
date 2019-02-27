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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.dinginfo.seamq.common.FileUtil;
import com.dinginfo.seamq.common.StringUtil;

/**
 * @author David Ding
 * 
 */
public class ServerConfig {
	public static final String STORAGE_HBASE="HBASE";
	
	public static final String STORAGE_JDBC="JDBC";
	
	private static final String fileName="../config/server.xml";
	
	private static final int brokerBucketSize =10; 

	private Map<String, String> map = new HashMap<String, String>();
	
	private boolean master;

	private static Map<String, String> globalMap;

	public ServerConfig() {
		map = loadProperty();
	}
	
	public ServerConfig(String file){
		InputStream is=null;
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			is = loader.getResourceAsStream(file);
			SAXReader saxReader = new SAXReader();
			Document doc = saxReader.read(is);
			loadProperty0(doc);
			map = globalMap;
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(is!=null){
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
	}

	private static synchronized Map<String, String> loadProperty() {
		if (globalMap != null) {
			return globalMap;
		}
		try {
			String xml = FileUtil.read(fileName);
			Document doc =DocumentHelper.parseText(xml);
			loadProperty0(doc);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return globalMap;
	}

	private static synchronized void loadProperty0(Document doc) throws Exception {
		if(globalMap !=null){
			return ;
		}
		Map<String, String> map = new HashMap<String, String>();
		
		Element rootEle = doc.getRootElement();
		List<Element> eleList = rootEle.elements("property");
		if (eleList == null) {
			return ;
		}
		String key = null;
		String value = null;
		for (Element e : eleList) {
			key = e.element("name").getText();
			if(key==null || key.trim().length()==0) {
				continue;
			}
			value = e.element("value").getTextTrim();
			map.put(key.toLowerCase(), value);
		}
		globalMap = map;
	}

	public int getBrokerPort() {
		String strPort = get("broker.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9090;
	}
	
	public int getMasterPort() {
		String strPort = get("master.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9091;
	}
	
	public int getMonitorPort() {
		String strPort = get("monitor.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9092;
	}

	public int getBrokerShutdownPort() {
		String strPort = get("broker.shutdown.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9005;
	}
	
	public int getMasterShutdownPort() {
		String strPort = get("master.shutdown.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9006;
	}
	
	public int getMonitorShutdownPort() {
		String strPort = get("monitor.shutdown.port");
		if (strPort != null && strPort.trim().length() > 0) {
			return Integer.parseInt(strPort);
		}
		return 9007;
	}

	public String getBrokerShutdownCommand() {
		String shutdown = get("broker.shutdown.command");
		if (shutdown != null && shutdown.trim().length() > 0) {
			return shutdown;
		}
		return "SHUTDOWN";
	}
	
	public String getMasterShutdownCommand() {
		String shutdown = get("master.shutdown.command");
		if (shutdown != null && shutdown.trim().length() > 0) {
			return shutdown;
		}
		return "MASTER_SHUTDOWN";
	}
	
	public String getMonitorShutdownCommand() {
		String shutdown = get("monitor.shutdown.command");
		if (shutdown != null && shutdown.trim().length() > 0) {
			return shutdown;
		}
		return "MONITOR_SHUTDOWN";
	}

	public long getSessionTime(){
		String time = get("broker.sessionTime");
		if(time!=null && time.trim().length()>0){
			return Long.parseLong(time);
		}else{
			return 10 * 1000;
		}
	}
	
	public boolean isNotifyMessage(){
		String notify = get("broker.notify");
		if(notify==null || notify.trim().length()==0){
			return true;
		}
		if("true".equalsIgnoreCase(notify.trim())){
			return true;
		}else{
			return false;
		}
	}
	
	/*
	 * return: minute
	 */
	public long getSessionTimeout() {
		String strTimeout = get("session.timeout");
		if (strTimeout != null && strTimeout.trim().length() > 0) {
			return Integer.parseInt(strTimeout) * 60 * 1000L;
		}
		return 120 * 60 * 1000L;
	}
	
	/*
	 * return: minute
	 */
	public long getSessionSaveInterval() {
		String strTimeout = get("session.saveInterval");
		if (strTimeout != null && strTimeout.trim().length() > 0) {
			return Integer.parseInt(strTimeout) * 60 * 1000L;
		}
		return 30 * 60 * 1000L;
	}

	/*
	 * return: minute
	 */
	public int getSechedulerInterval() {
		String strInterval = get("secheduler.interval");
		if (strInterval != null && strInterval.trim().length() > 0) {
			return Integer.parseInt(strInterval);
		}
		return 30;
	}

	
	/*
	 * return: minute
	 */
	public Map<String,String> getSeckeyMap() {
		Map<String, String> seckeyMap = new HashMap<String, String>();
		String seckeys = get("security.keys");
		String key=null;
		try {
			key=StringUtil.generateMD5String("seamq-security");
			seckeyMap.put(key, key);
			if(seckeys==null || seckeys.trim().length()==0){
				return seckeyMap;
			}else{
				String[] keyArray=seckeys.split(",");
				for(String str : keyArray){
					key=StringUtil.generateMD5String(str);
					seckeyMap.put(key, key);
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return seckeyMap;
		
	}
	
	
	/*
	 * return: zookeeper root
	 */
	public String getMQRoot() {
		String root = get("zookeeper.root");
		if (root != null && root.trim().length() > 0) {
			return root;
		}else{
			return "SeaMQ";
		}
		
	}

	
	/*
	 * return: zookeeper URLs
	 */
	public String getZookeeperURLs() {
		String urls = get("zookeeper.urls");
		if (urls != null && urls.trim().length() > 0) {
			return urls;
		}else{
			return "";
		}
		
	}
	
	/*
	 * return: zookeeper URLs
	 */
	public int getVirtualBrokerCopys() {
		String num = get("master.virtualBrokerCopys");
		if (num != null && num.trim().length() > 0) {
			return Integer.parseInt(num);
		}else{
			return 160;
		}
		
	}

	public boolean isMaster() {
		return master;
	}

	public void setMaster(boolean master) {
		this.master = master;
	}
	
	public String getJDBCDataSourceClassName(){
		return get("storage.jdbc.dataSource.className");
	}
	
	public String getJDBCClassName(){
		return get("storage.jdbc.className");
	}
	
	public String getJDBCUrl(){
		return get("storage.jdbc.url");
	}
	
	public String getJDBCUserName(){
		return get("storage.jdbc.username");
	}
	
	public String getJDBCPassword(){
		return get("storage.jdbc.password");
	}
	
	public int getJDBCInitPoolSize(){
		String size=get("storage.jdbc.initPoolSize");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size);
		}else{
			return 10;	
		}
	}
	
	public int getJDBCMinPoolSize(){
		String size=get("storage.jdbc.minPoolSize");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size);
		}else{
			return 10;	
		}
	}
	
	public int getJDBCMaxPoolSize(){
		String size=get("storage.jdbc.maxPoolSize");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size);
		}else{
			return 20;	
		}
	}
	
	public String getHBaseZookeeperQuorum(){
		return get("storage.hbase.zookeeper.quorum");
	}
	
	
	
	public int getHBaseZookeeperClientPort(){
		String port = get("storage.hbase.zookeeper.clientPort");
		if(port!=null && port.trim().length()>0){
			return Integer.parseInt(port.trim());
		}else{
			return 0;
		}
	}
	
	public String getHBaseNamespace(){
		String namespace = get("storage.hbase.namespace");
		if(namespace!=null && namespace.trim().length()>0){
			return namespace;
		}else{
			return "seamq";
		}
	}
	
	public String getStorageType(){
		String type=get("storage.type");
		if(type!=null && type.trim().length()>0){
			return type.trim();
		}else{
			return STORAGE_HBASE;	
		}
	}
	
	public String getLocalStoragePath(){
		return get("storage.local.path");
	}
	
	public int getBrokerBucketSize(){
		return brokerBucketSize;
	}
	
	public int getBossThreadPoolSize(){
		String size = get("threadPool.boss.size");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size.trim());
		}else{
			return 0;	
		}
	}
	
	public int getWorkThreadPoolSize(){
		String size = get("threadPool.work.size");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size.trim());
		}else{
			return 200;	
		}
	}
	
	public int getTaskThreadPoolSize(){
		String size = get("threadPool.task.size");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size.trim());
		}else{
			return 200;	
		}
	}
	
	public int getMaxTaskQueueSize(){
		String size = get("max.taskQueue.size");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size.trim());
		}else{
			return 1000 * 1000;	
		}
	}
	
	public int getNotifyThreadPoolSize(){
		String size = get("threadPool.notify.size");
		if(size!=null && size.trim().length()>0){
			return Integer.parseInt(size.trim());
		}else{
			return 10;	
		}
	}
	
	public String getHDFSDefaultFS(){
		return get("fs.defaultFS");
	}
	
	private String get(String key) {
		return map.get(key.toLowerCase());
	}
	
}
