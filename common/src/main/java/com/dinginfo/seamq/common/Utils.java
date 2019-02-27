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

import java.security.MessageDigest;


/** 
 * @author David Ding
 *
 */
public class Utils {
	public static String getMD5String(String str) {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(str.getBytes());
			byte[] b = md5.digest();
			return byte2hex(b);
		} catch (Exception ex) {
			return null;
		}

	}
	
	public static String get16MD5String(String str) {
		String s=getMD5String(str);
		if(s!=null && s.length()==32){
			return s.substring(8, 24);
		}else{
			return null;
		}
	}

	private static String byte2hex(byte[] b) {
		String hs = "";
		StringBuffer sb = new StringBuffer();
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
		return hs.toUpperCase();
	}
}
