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

package com.dinginfo.seamq.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;


/** 
 * @author David Ding
 *
 */
public class NodeAddress {
	 List<InetSocketAddress> addressList;

		public List<InetSocketAddress> getAddressList() {
			return addressList;
		}

		public void setAddressList(List<InetSocketAddress> addressList) {
			this.addressList = addressList;
		}
		
		public void setAddress(String address){
			addressList=new ArrayList<InetSocketAddress>();
			String ipAddress[] = address.split(",");
			for (int i = 0, n = ipAddress.length; i < n; i++) {
				String[] strList = ipAddress[i].split(":");
				if (strList.length > 1) {
					String ip = strList[0];
					String port = strList[1];
					addressList.add(new InetSocketAddress(ip, Integer.parseInt(port)));
				}
			}
		}
}
