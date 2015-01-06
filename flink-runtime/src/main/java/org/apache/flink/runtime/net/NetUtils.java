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

package org.apache.flink.runtime.net;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

	/**
	 * The states of address detection mechanism.
	 * There is only a state transition if the current state failed to determine the address.
	 */
	private enum AddressDetectionState {
		ADDRESS(50), 		//detect own IP based on the JobManagers IP address. Look for common prefix
		FAST_CONNECT(50),	//try to connect to the JobManager on all Interfaces and all their addresses.
		//this state uses a low timeout (say 50 ms) for fast detection.
		SLOW_CONNECT(1000),	//same as FAST_CONNECT, but with a timeout of 1000 ms (1s).
		HEURISTIC(0);


		private int timeout;
		AddressDetectionState(int timeout) {
			this.timeout = timeout;
		}
		public int getTimeout() {
			return timeout;
		}
	}

	/**
	 * Find out the TaskManager's own IP address.
	 */
	public static InetAddress resolveAddress(InetSocketAddress jobManagerAddress) throws IOException {
		AddressDetectionState strategy = jobManagerAddress != null ? AddressDetectionState.ADDRESS: AddressDetectionState.HEURISTIC;

		while (true) {
			Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
			
			while (e.hasMoreElements()) {
				NetworkInterface n = e.nextElement();
				Enumeration<InetAddress> ee = n.getInetAddresses();
				
				while (ee.hasMoreElements()) {
					InetAddress i = ee.nextElement();
					
					switch (strategy) {
						case ADDRESS:
							if (hasCommonPrefix(jobManagerAddress.getAddress().getAddress(), i.getAddress())) {
								if (tryToConnect(i, jobManagerAddress, strategy.getTimeout())) {
									LOG.info("Determined " + i + " as the TaskTracker's own IP address");
									return i;
								}
							}
							break;
							
						case FAST_CONNECT:
						case SLOW_CONNECT:
							boolean correct = tryToConnect(i, jobManagerAddress, strategy.getTimeout());
							if (correct) {
								LOG.info("Determined " + i + " as the TaskTracker's own IP address");
								return i;
							}
							break;
							
						case HEURISTIC:
							if (LOG.isDebugEnabled()) {
								LOG.debug("ResolveAddress using heuristic strategy for " + i + " with" +
										" isLinkLocalAddress:" + i.isLinkLocalAddress() +
										" isLoopbackAddress:" + i.isLoopbackAddress() + ".");
							}
							
							if (!i.isLinkLocalAddress() && !i.isLoopbackAddress() && i instanceof Inet4Address){
								LOG.warn("Hostname " + InetAddress.getLocalHost().getHostName() + " resolves to " +
										"loopback address. Using instead " + i.getHostAddress() + " on network " +
										"interface " + n.getName() + ".");
								return i;
							}
							break;
							
						default:
							throw new RuntimeException("Unkown address detection strategy: " + strategy);
					}
				}
			}
			// state control
			switch (strategy) {
				case ADDRESS:
					strategy = AddressDetectionState.FAST_CONNECT;
					break;
				case FAST_CONNECT:
					strategy = AddressDetectionState.SLOW_CONNECT;
					break;
				case SLOW_CONNECT:
					if (!InetAddress.getLocalHost().isLoopbackAddress()) {
						LOG.info("Heuristically taking " + InetAddress.getLocalHost() + " as own " +
								"IP address.");
						return InetAddress.getLocalHost();
					} else {
						strategy = AddressDetectionState.HEURISTIC;
						break;
					}
				case HEURISTIC:
					throw new RuntimeException("Unable to resolve own inet address by connecting " +
							"to address (" + jobManagerAddress + ").");
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Defaulting to detection strategy " + strategy);
			}
		}
	}

	/**
	 * Checks if two addresses have a common prefix (first 2 bytes).
	 * Example: 192.168.???.???
	 * Works also with ipv6, but accepts probably too many addresses
	 */
	private static boolean hasCommonPrefix(byte[] address, byte[] address2) {
		return address[0] == address2[0] && address[1] == address2[1];
	}

	public static boolean tryToConnect(InetAddress fromAddress, SocketAddress toSocket, int timeout) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to connect to JobManager (" + toSocket + ") from local address " + fromAddress
					+ " with timeout " + timeout);
		}
		boolean connectable = true;
		Socket socket = null;
		try {
			socket = new Socket();
			SocketAddress bindP = new InetSocketAddress(fromAddress, 0); // 0 = let the OS choose the port on this
			// machine
			socket.bind(bindP);
			socket.connect(toSocket, timeout);
		} catch (Exception ex) {
			LOG.info("Failed to connect to JobManager from address '" + fromAddress + "': " + ex.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Failed with exception", ex);
			}
			connectable = false;
		}
		finally {
			if (socket != null) {
				socket.close();
			}
		}
		return connectable;
	}

	public static final int getAvailablePort() {
		for (int i = 0; i < 50; i++) {
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(0);
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			}
			catch (IOException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unable to allocate port " + e.getMessage(), e);
				}
			}
			finally {
				if (serverSocket != null) {
					try {
						serverSocket.close();
					} catch (Throwable t) {
						// ignored
					}
				}
			}
		}

		throw new RuntimeException("Could not find a free permitted port on the machine.");
	}
}
