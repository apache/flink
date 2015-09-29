/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.util;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;

public class NetUtils {
	
	/**
	 * Turn a fully qualified domain name (fqdn) into a hostname. If the fqdn has multiple subparts
	 * (separated by a period '.'), it will take the first part. Otherwise it takes the entire fqdn.
	 * 
	 * @param fqdn The fully qualified domain name.
	 * @return The hostname.
	 */
	public static String getHostnameFromFQDN(String fqdn) {
		if (fqdn == null) {
			throw new IllegalArgumentException("fqdn is null");
		}
		int dotPos = fqdn.indexOf('.');
		if(dotPos == -1) {
			return fqdn;
		} else {
			return fqdn.substring(0, dotPos);
		}
	}

	/**
	 * Method to validate if the given String represents a hostname:port.
	 *
	 * Works also for ipv6.
	 *
	 * See: http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
	 *
	 * @return URL object for accessing host and Port
	 */
	public static URL getCorrectHostnamePort(String hostPort) {
		try {
			URL u = new URL("http://"+hostPort);
			if(u.getHost() == null) {
				throw new IllegalArgumentException("The given host:port ('"+hostPort+"') doesn't contain a valid host");
			}
			if(u.getPort() == -1) {
				throw new IllegalArgumentException("The given host:port ('"+hostPort+"') doesn't contain a valid port");
			}
			return u;
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("The given host:port ('"+hostPort+"') is invalid", e);
		}
	}

	/**
	 * Find a non-occupied port.
	 *
	 * @return A non-occupied port.
	 */
	public static int getAvailablePort() {
		for (int i = 0; i < 50; i++) {
			try (ServerSocket serverSocket = new ServerSocket(0)) {
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			}
			catch (IOException ignored) {}
		}

		throw new RuntimeException("Could not find a free permitted port on the machine.");
	}
}
