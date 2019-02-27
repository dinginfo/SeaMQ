/*
 * Copyright 2011 dingframework Team.
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

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** 
 * @author David Ding
 *
 */

public class UUIDHexGenerator {

	private static final int IP;
	static {
		int ipadd;
		try {
			ipadd = toInt(InetAddress.getLocalHost().getAddress());
		} catch (Exception e) {
			ipadd = 0;
		}
		IP = ipadd;
	}
	
	
	
	private static short counter = (short) 0;
	private static long longCounter=0L;
	private static int intCounter=0;
	private static final int JVM = (int) (System.currentTimeMillis() >>> 8);

	/**
	 * Unique across JVMs on this machine (unless they load this class in the
	 * same quater second - very unlikely)
	 */
	private static int getJVM() {
		return JVM;
	}

	/**
	 * Unique in a millisecond for this JVM instance (unless there are >
	 * Short.MAX_VALUE instances created in a millisecond)
	 */
	private static synchronized short getShortCount() {
		if (counter < 0)
			counter = 0;
		return counter++;
	}
	
	private static synchronized int getIntCount() {
		if (intCounter < 0)
			intCounter = 0;
		return intCounter++;
	}
	
	private static synchronized long getLongCount() {
		if (longCounter < 0)
			longCounter = 0;
		return longCounter++;
	}

	/**
	 * Unique in a local network
	 */
	private static int getIP() {
		return IP;
	}

	/**
	 * Unique down to millisecond
	 */
	private static short getHiTime() {
		return (short) (System.currentTimeMillis() >>> 32);
	}

	private static int getLoTime() {
		return (int) System.currentTimeMillis();
	}

	private String sep = "";

	private static String formatInt(int intval) {
		String formatted = Integer.toHexString(intval);
		StringBuffer buf = new StringBuffer("00000000");
		buf.replace(8 - formatted.length(), 8, formatted);
		return buf.toString();
	}

	private static String formatShort(short shortval) {
		String formatted = Integer.toHexString(shortval);
		StringBuffer buf = new StringBuffer("0000");
		buf.replace(4 - formatted.length(), 4, formatted);
		return buf.toString();
	}
	
	
	private static String formatLong(long longval) {
		String formatted = Long.toHexString(longval);
		StringBuffer buf = new StringBuffer("0000000000000000");
		buf.replace(16 - formatted.length(), 16, formatted);
		return buf.toString();
	}
	
	private static int toInt( byte[] bytes ) {
		int result = 0;
		for (int i=0; i<4; i++) {
			result = ( result << 8 ) - Byte.MIN_VALUE + (int) bytes[i];
		}
		return result;
	}
	
	private static String byte2hex(byte[] b) {
		String hs = "";
		StringBuffer sb = new StringBuffer(b.length * 2);
		String stmp = "";
		for (int i = 0; i < b.length; i++) {
			stmp = (java.lang.Integer.toHexString(b[i] & 0XFF));
			if (stmp.length() == 1) {
				sb.append("0");
				sb.append(stmp);
			} else {
				sb.append(stmp);
			}
		}
		hs = sb.toString();
		return hs.toLowerCase();
	}

	private static byte[] generateMD5Byte(String str){
		MessageDigest md5=null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		md5.update(str.getBytes());
		return md5.digest();
	}
	
	private static void generateBasic(StringBuilder sb) {
		sb.append(formatInt(getIP()));
		sb.append(formatInt(getJVM()));
		sb.append(formatShort(getHiTime()));
		sb.append(formatInt(getLoTime()));
	}
	
	public static String generate() {
		StringBuilder sb = new StringBuilder(36);
		generateBasic(sb);
		sb.append(formatShort(getShortCount()));
		return sb.toString();
	}
	
	public static String generateInt() {
		StringBuilder sb = new StringBuilder(40);
		generateBasic(sb);
		sb.append(formatInt(getIntCount()));
		return sb.toString();
	}
	
	public static String generateLong(){
		StringBuilder sb = new StringBuilder(64);
		generateBasic(sb);
		sb.append(formatLong(getLongCount()));
		return sb.toString();
	}
	
	public static String generateShortMD5String(){
		byte[] bytes=generateMD5Byte(generate());
		return byte2hex(bytes);
	}
	
	public static String generateIntMD5String(){
		byte[] bytes=generateMD5Byte(generateInt());
		return byte2hex(bytes);
	}
	
	public static String generateLongMD5String(){
		byte[] bytes=generateMD5Byte(generateLong());
		return byte2hex(bytes);
	}

	public static void main(String[] args) {
		long time=System.currentTimeMillis();
		String id=null;
		for (int i=0;i<1000000;i++){
			 id=UUIDHexGenerator.generateLongMD5String();
			
			//System.out.println(id);
		}
		System.out.println(System.currentTimeMillis()-time);
		

	}

}
