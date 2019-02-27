/*
 * Copyright 2011 David Ding.
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



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;




/** 
 * @author David Ding
 *
 */


public class StringUtil {
	public static String appendString(String str1, String str2) {
		StringBuilder sb = new StringBuilder(200);
		sb.append(str1);
		sb.append("-");
		sb.append(str2);
		return sb.toString();
	}
	
	public static String composeString(String... strings){
		StringBuilder sb = new StringBuilder(200);
		for(String str:strings){
			sb.append(str);
		}
		return sb.toString();
	}

	public static String object2String(Object obj)throws Exception{
		return object2String(obj, "UTF-8");
	}
	public static String object2String(Object obj, String charsetName)
			throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		bos.close();
		
		return Base64.encodeBase64String(bos.toByteArray());
	}
	
	public static Object string2Object(String str)throws Exception{
		return string2Object(str, "UTF-8");
	}

	public static Object string2Object(String str, String charsetName)
			throws Exception {
		ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decodeBase64(str));
		ObjectInputStream ois = new ObjectInputStream(bis);
		Object obj = ois.readObject();
		bis.close();
		ois.close(); 
		return obj;
	}
	
	public static byte[] generateMD5Byte(String str){
		MessageDigest md5=null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		md5.update(str.getBytes());
		return md5.digest();
	}
	
	public static String generateMD5String(String str)throws Exception{
		return md(str, "MD5");
	}
	
	public static String generateSHA512String(String str)throws Exception{
		return md(str, "SHA-512");
	}
	
	public static String generateSHA256String(String str)throws Exception{
		return md(str, "SHA-256");
	}
	
	private static String md(String str,String type)throws Exception{
		MessageDigest md = MessageDigest.getInstance(type);
        md.update(str.getBytes("UTF-8"));
        byte[] bytes = md.digest();
		return byte2hex(bytes);
	}
	
	public static String byte2hex(byte[] b) {
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
		return sb.toString();
	}
	
	public static void main(String[] args)throws Exception{
		String a ="1";
		String str = generateSHA256String(a);
		System.out.println(str.length());
		System.out.println(str);
	}
	
	
}
