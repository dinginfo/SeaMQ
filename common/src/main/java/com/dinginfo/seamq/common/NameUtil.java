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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NameUtil {
	
	public static boolean check(String name){
		if(name==null){
			return false;
		}
		char c;
		for(int i=0,n=name.length();i<n;i++){
			c=name.charAt(i);
			if(!isNumber(c) && !isAtoZ(c) && !isOtherChar(c)){
				return false;
			}
		}
		return true;
	}
	
	private static boolean isNumber(char value){
		if(value>=48 && value<=57){
			return true;
		}else{
			return false;
		}
	}
	
	private static boolean isAtoZ(char value){
		if(value>=65 && value <=90){
			return true;
		}else if(value>=97 && value<=122){
			return true;
		}else{
			return false;
		}
	}
	
	private static boolean isOtherChar(char value){
		boolean result=false;
		switch(value){
		case 35:
			result = true;
			break;
		case 40:
			result = true;
			break;
		case 41:
			result = true;
			break;
		case 45:
			result = true;
			break;
		case 46:
			result = true;
			break;
		case 58:
			result = true;
			break;
		case 59:
			result = true;
			break;
		case 91:
			result = true;
			break;
		case 93:
			result = true;
			break;
		case 95:
			result = true;
			break;
		case 124:
			result = true;
			break;
		}
		return result;
	}

}
