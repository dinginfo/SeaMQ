package com.dinginfo.seamq.master;

import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.dinginfo.seamq.ServerConfig;
import com.dinginfo.seamq.client.MQConfig;
import com.dinginfo.seamq.common.MyBean;
import com.dinginfo.seamq.common.StringUtil;
import com.dinginfo.seamq.entity.NodeInfo;
import com.dinginfo.seamq.scheduler.ClearSessionTask;
import com.dinginfo.seamq.scheduler.RegisterZkNode;
import com.dinginfo.seamq.service.SessionService;

public class MonitorServer{
	private final static Logger logger = LogManager
			.getLogger(MonitorServer.class);

	private long SESSION_TIMEOUT = 120 * 60 * 1000;
	
	private String monitorId=null;

	private ZkClient zkclient;

	private ClearSessionTask clearSessionTask;

	private SessionService sessionService;
	
	private RegisterZkNode monitorRegister;
	
	private ScheduledExecutorService scheduledService ;
	
	private ServerConfig serverConfig ;

	public MonitorServer(ServerConfig config) {
		this.serverConfig = config;
		MQConfig clientConfig = new MQConfig();
		clientConfig.setMQRoot(config.getMQRoot());
		clientConfig.setZookeeperURL(config.getZookeeperURLs());
		this.scheduledService = Executors.newScheduledThreadPool(2);
	}
	
	public void init()throws Exception{
		zkclient = new ZkClient(serverConfig.getZookeeperURLs());
		sessionService = MyBean.getBean(SessionService.BEAN_NAME, SessionService.class);
		clearSessionTask = new ClearSessionTask(sessionService, SESSION_TIMEOUT);
		scheduledService.scheduleAtFixedRate(clearSessionTask, 10, 720, TimeUnit.MINUTES);
		
	}

	
	public void registerMonitor(ServerConfig config)throws Exception{
		 InetAddress address = InetAddress.getLocalHost();
		 String ip =address.getHostAddress();
		 String hostName = address.getHostName();
		 int port = config.getMonitorPort();
		 NodeInfo node = new NodeInfo();
		 node.setHost(hostName);
		 node.setIp(ip);
		 node.setPort(port);
		 StringBuilder sb = new StringBuilder();
		 sb.append(hostName);
		 sb.append(":");
		 sb.append(port);
		 node.setId(StringUtil.generateMD5String(sb.toString()));
		 monitorId = node.getId();
		 String jsonString = JSON.toJSONString(node);
		 
		 String zkurl = config.getZookeeperURLs();
		 String zkRoot =config.getMQRoot();
		 sb = new StringBuilder();
		 if(!"/".equals(zkRoot.substring(0, 1))){
			 sb.append("/");	 
		 }
		 sb.append(zkRoot);
		 
		 zkRoot = sb.toString();
		 if(!zkclient.exists(zkRoot)){
			 zkclient.createPersistent(zkRoot);
		 }
		 
		 sb.append("/monitor");
		 String monitorPath=sb.toString();
		 boolean registed=false; 
		 long sleepTime = 1000 * 10;
		 int sleepCount =0;
		 NodeInfo masterNode =null;
		 String monitorJson =null;
		 while(true){
			 registed =zkclient.exists(monitorPath);
			 if(!registed){
				 zkclient.createEphemeral(monitorPath, jsonString);
				 monitorJson =zkclient.readData(monitorPath);
				 if(monitorJson!=null){
					 masterNode = JSON.parseObject(monitorJson, NodeInfo.class);
					 if(masterNode!=null && monitorId.equals(masterNode.getId())){
						 break;	 
					 }
				 }
				 
			 }
			 Thread.sleep(sleepTime);
			 sleepCount +=1;
			 if(sleepCount>=6){
				 sleepCount =0;
				 logger.info("register Monitor,sleep 60 second");	 
			 }
		 }
		 monitorRegister = new RegisterZkNode(zkclient, monitorPath, monitorJson);
		 
		 scheduledService.scheduleAtFixedRate(monitorRegister, 60, 10, TimeUnit.SECONDS);
	}
	
	
	public void shutdown() {		
		if(zkclient!=null){
			zkclient.close();
		}
		if(scheduledService!=null){
			scheduledService.shutdown();
		}
	}
	
	public static void main(String[] args) {
		MyBean.start();
		ServerConfig config = new ServerConfig();
		config.setMaster(false);
		final MonitorServer server = new MonitorServer(config);
		
		try {
			logger.info("starting monitor service");
			server.init();
			server.registerMonitor(config);
			//shutdown listen server
			String shutdown = config.getMonitorShutdownCommand();
			int port = config.getMonitorShutdownPort();
			MonitorShutdownListener stopListener = new MonitorShutdownListener(server, shutdown, port);
			stopListener.start();
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					server.shutdown();
				}
			}));
			logger.info("start monitor service, shutdown port:"+port);
		} catch (Exception e) {
			logger.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
	}
}
