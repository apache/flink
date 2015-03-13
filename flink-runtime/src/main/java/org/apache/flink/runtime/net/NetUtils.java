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

/**
 * Utilities to determine the network interface and address that should be used to bind the
 * TaskManager communication to.
 */
public class NetUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

	private static final long MIN_SLEEP_TIME = 50;
	private static final long MAX_SLEEP_TIME = 20000;

	/**
	 * The states of address detection mechanism.
	 * There is only a state transition if the current state failed to determine the address.
	 */
	private enum AddressDetectionState {
		/** Detect own IP address based on the target IP address. Look for common prefix */
		ADDRESS(50),
		/** Try to connect on all Interfaces and all their addresses with a low timeout */
		FAST_CONNECT(50),
		/** Try to connect on all Interfaces and all their addresses with a long timeout */
		SLOW_CONNECT(1000),
		/** Choose any non-loopback address */
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
	 * Find out the TaskManager's own IP address, simple version.
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
								if (tryToConnect(i, jobManagerAddress, strategy.getTimeout(), true)) {
									LOG.info("Determined {} as the machine's own IP address", i);
									return i;
								}
							}
							break;

						case FAST_CONNECT:
						case SLOW_CONNECT:
							boolean correct = tryToConnect(i, jobManagerAddress, strategy.getTimeout(), true);
							if (correct) {
								LOG.info("Determined {} as the machine's own IP address", i);
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
							throw new RuntimeException("Unknown address detection strategy: " + strategy);
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
	 * Finds the local network address from which this machine can connect to the target
	 * address. This method tries to establish a proper network connection to the
	 * given target, so it only succeeds if the target socket address actually accepts
	 * connections. The method tries various strategies multiple times and uses an exponential
	 * backoff timer between tries.
	 * <p>
	 * If no connection attempt was successful after the given maximum time, the method
	 * will choose some address based on heuristics (excluding link-local and loopback addresses.)
	 * <p>
	 * This method will initially not log on info level (to not flood the log while the
	 * backoff time is still very low). It will start logging after a certain time
	 * has passes.
	 *
	 * @param targetAddress The address that the method tries to connect to.
	 * @param maxWaitMillis The maximum time that this method tries to connect, before falling
	 *                       back to the heuristics.
	 * @param startLoggingAfter The time after which the method will log on INFO level.
	 */
	public static InetAddress findConnectingAddress(InetSocketAddress targetAddress,
							long maxWaitMillis, long startLoggingAfter) throws IOException
	{
		if (targetAddress == null) {
			throw new NullPointerException("targetAddress must not be null");
		}
		if (maxWaitMillis <= 0) {
			throw new IllegalArgumentException("Max wait time must be positive");
		}

		final long startTime = System.currentTimeMillis();

		long currentSleepTime = MIN_SLEEP_TIME;
		long elapsedTime = 0;

		// loop while there is time left
		while (elapsedTime < maxWaitMillis) {
			AddressDetectionState strategy = AddressDetectionState.ADDRESS;

			boolean logging = elapsedTime >= startLoggingAfter;
			if (logging) {
				LOG.info("Trying to connect to " + targetAddress);
			}
			// go over the strategies ADDRESS - FAST_CONNECT - SLOW_CONNECT
			do {
				InetAddress address = findAddressUsingStrategy(strategy, targetAddress, logging);
				if (address != null) {
					return address;
				}

				// pick the next strategy
				switch (strategy) {
					case ADDRESS:
						strategy = AddressDetectionState.FAST_CONNECT;
						break;
					case FAST_CONNECT:
						strategy = AddressDetectionState.SLOW_CONNECT;
						break;
					case SLOW_CONNECT:
						strategy = null;
						break;
					default:
						throw new RuntimeException("Unsupported strategy: " + strategy);
				}
			}
			while (strategy != null);

			// we have made a pass with all strategies over all interfaces
			// sleep for a while before we make the next pass
			elapsedTime = System.currentTimeMillis() - startTime;

			long toWait = Math.min(maxWaitMillis - elapsedTime, currentSleepTime);
			if (toWait > 0) {
				if (logging) {
					LOG.info("Could not connect. Waiting for {} msecs before next attempt", toWait);
				} else {
					LOG.debug("Could not connect. Waiting for {} msecs before next attempt", toWait);
				}

				try {
					Thread.sleep(toWait);
				}
				catch (InterruptedException e) {
					throw new IOException("Connection attempts have been interrupted.");
				}
			}

			// increase the exponential backoff timer
			currentSleepTime = Math.min(2 * currentSleepTime, MAX_SLEEP_TIME);
		}

		// our attempts timed out. use the heuristic fallback
		LOG.warn("Could not connect to {}. Selecting a local address using heuristics.", targetAddress);
		InetAddress heuristic = findAddressUsingStrategy(AddressDetectionState.HEURISTIC, targetAddress, true);
		if (heuristic != null) {
			return heuristic;
		}
		else {
			LOG.warn("Could not find any IPv4 address that is not loopback or link-local. Using localhost address.");
			return InetAddress.getLocalHost();
		}
	}


	private static InetAddress findAddressUsingStrategy(AddressDetectionState strategy,
														InetSocketAddress targetAddress,
														boolean logging) throws IOException
	{
		final byte[] targetAddressBytes = targetAddress.getAddress().getAddress();

		// for each network interface
		Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
		while (e.hasMoreElements()) {

			NetworkInterface netInterface = e.nextElement();

			// for each address of the network interface
			Enumeration<InetAddress> ee = netInterface.getInetAddresses();
			while (ee.hasMoreElements()) {
				InetAddress interfaceAddress = ee.nextElement();

				switch (strategy) {
					case ADDRESS:
						if (hasCommonPrefix(targetAddressBytes, interfaceAddress.getAddress())) {
							LOG.debug("Target address {} and local address {} share prefix - trying to connect.",
										targetAddress, interfaceAddress);

							if (tryToConnect(interfaceAddress, targetAddress, strategy.getTimeout(), logging)) {
								return interfaceAddress;
							}
						}
						break;

					case FAST_CONNECT:
					case SLOW_CONNECT:
						LOG.debug("Trying to connect to {} from local address {} with timeout {}",
								targetAddress, interfaceAddress, strategy.getTimeout());

						if (tryToConnect(interfaceAddress, targetAddress, strategy.getTimeout(), logging)) {
							return interfaceAddress;
						}
						break;

					case HEURISTIC:
						if (LOG.isDebugEnabled()) {
							LOG.debug("Checking address {} using heuristics: linkLocal: {} loopback: {}",
									interfaceAddress, interfaceAddress.isLinkLocalAddress(),
									interfaceAddress.isLoopbackAddress());
						}
						// pick a non-loopback non-link-local address
						if (interfaceAddress instanceof Inet4Address && !interfaceAddress.isLinkLocalAddress() &&
								!interfaceAddress.isLoopbackAddress())
						{
							return interfaceAddress;
						}
						break;

					default:
						throw new RuntimeException("Unsupported strategy: " + strategy);
				}
			} // end for each address of the interface
		} // end for each interface

		return null;
	}

	/**
	 * Checks if two addresses have a common prefix (first 2 bytes).
	 * Example: 192.168.???.???
	 * Works also with ipv6, but accepts probably too many addresses
	 */
	private static boolean hasCommonPrefix(byte[] address, byte[] address2) {
		return address[0] == address2[0] && address[1] == address2[1];
	}

	/**
	 *
	 * @param fromAddress The address to connect from.
	 * @param toSocket The socket address to connect to.
	 * @param timeout The timeout fr the connection.
	 * @param logFailed Flag to indicate whether to log failed attempts on info level
	 *                  (failed attempts are always logged on DEBUG level).
	 * @return True, if the connection was successful, false otherwise.
	 * @throws IOException Thrown if the socket cleanup fails.
	 */
	private static boolean tryToConnect(InetAddress fromAddress, SocketAddress toSocket,
										int timeout, boolean logFailed) throws IOException
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to connect to (" + toSocket + ") from local address " + fromAddress
					+ " with timeout " + timeout);
		}
		Socket socket = new Socket();
		try {
			// port 0 = let the OS choose the port
			SocketAddress bindP = new InetSocketAddress(fromAddress, 0);
			// machine
			socket.bind(bindP);
			socket.connect(toSocket, timeout);
			return true;
		}
		catch (Exception ex) {
			String message = "Failed to connect from address '" + fromAddress + "': " + ex.getMessage();
			if (LOG.isDebugEnabled()) {
				LOG.debug(message, ex);
			} else if (logFailed) {
				LOG.info(message);
			}
			return false;
		}
		finally {
			socket.close();
		}
	}

	/**
	 * Find a non-occupied port.
	 *
	 * @return A non-occupied port.
	 */
	public static int getAvailablePort() {
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
