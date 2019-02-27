package com.dinginfo.seamq.client.exception;

public class TimeoutException extends Exception {
	
	public TimeoutException(){}
	
	public TimeoutException(String message){
		super(message);
	}
}
