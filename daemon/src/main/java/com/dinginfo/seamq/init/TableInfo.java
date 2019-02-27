package com.dinginfo.seamq.init;

public class TableInfo {
	private String namespace;
	
	private String name;
	
	private int regions;
	
	public TableInfo(){}
	
	public TableInfo(String namespace,String tableName){
		this.namespace = namespace;
		this.name = tableName;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getRegions() {
		return regions;
	}

	public void setRegions(int regions) {
		this.regions = regions;
	}
	
	public String getTableName(){
		if(namespace==null || namespace.trim().length()==0){
			return name;
		}else{
			StringBuilder sb = new StringBuilder(50);
			sb.append(namespace);
			sb.append(":");
			sb.append(name);
			return sb.toString();
		}
		
	}
	

}
