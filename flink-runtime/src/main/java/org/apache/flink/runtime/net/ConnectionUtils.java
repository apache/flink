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

import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

/**
 * Utilities to determine the network interface and address that should be used to bind the
 * TaskManager communication to.
 *
 * <p>Implementation note: This class uses {@code System.nanoTime()} to measure elapsed time, because
 * that is not susceptible to clock changes.
 */
public class ConnectionUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtils.class);

	private static final long MIN_SLEEP_TIME = 50;
	private static final long MAX_SLEEP_TIME = 20000;

	/**
	 * The states of address detection mechanism.
	 * There is only a state transition if the current state failed to determine the address.
	 */
	private enum AddressDetectionState {
		/** Connect from interface returned by InetAddress.getLocalHost(). **/
		LOCAL_HOST(200),
		/** Detect own IP address based on the target IP address. Look for common prefix */
		ADDRESS(50),
		/** Try to connect on all Interfaces and all their addresses with a low timeout. */
		FAST_CONNECT(50),
		/** Try to connect on all Interfaces and all their addresses with a long timeout. */
		SLOW_CONNECT(1000),
		/** Choose any non-loopback address. */
		HEURISTIC(0);

		private final int timeout;

		AddressDetectionState(int timeout) {
			this.timeout = timeout;
		}

		public int getTimeout() {
			return timeout;
		}
	}


	/**
	 * Finds the local network address from which this machine can connect to the target
	 * address. This method tries to establish a proper network connection to the
	 * given target, so it only succeeds if the target socket address actually accepts
	 * connections. The method tries various strategies multiple times and uses an exponential
	 * backoff timer between tries.
	 *
	 * <p>If no connection attempt was successful after the given maximum time, the method
	 * will choose some address based on heuristics (excluding link-local and loopback addresses.)
	 *
	 * <p>This method will initially not log on info level (to not flood the log while the
	 * backoff time is still very low). It will start logging after a certain time
	 * has passes.
	 *
	 * @param targetAddress The address that the method tries to connect to.
	 * @param maxWaitMillis The maximum time that this method tries to connect, before falling
	 *                       back to the heuristics.
	 * @param startLoggingAfter The time after which the method will log on INFO level.
	 */
	public static InetAddress findConnectingAddress(InetSocketAddress targetAddress,
							long maxWaitMillis, long startLoggingAfter) throws IOException {
		if (targetAddress == null) {
			throw new NullPointerException("targetAddress must not be null");
		}
		if (maxWaitMillis <= 0) {
			throw new IllegalArgumentException("Max wait time must be positive");
		}

		final long startTimeNanos = System.nanoTime();

		long currentSleepTime = MIN_SLEEP_TIME;
		long elapsedTimeMillis = 0;

		final List<AddressDetectionState> strategies = Collections.unmodifiableList(
			Arrays.asList(
				AddressDetectionState.LOCAL_HOST,
				AddressDetectionState.ADDRESS,
				AddressDetectionState.FAST_CONNECT,
				AddressDetectionState.SLOW_CONNECT));

		// loop while there is time left
		while (elapsedTimeMillis < maxWaitMillis) {
			boolean logging = elapsedTimeMillis >= startLoggingAfter;
			if (logging) {
				LOG.info("Trying to connect to " + targetAddress);
			}

			// Try each strategy in order
			for (AddressDetectionState strategy : strategies) {
				InetAddress address = findAddressUsingStrategy(strategy, targetAddress, logging);
				if (address != null) {
					return address;
				}
			}

			// we have made a pass with all strategies over all interfaces
			// sleep for a while before we make the next pass
			elapsedTimeMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;

			long toWait = Math.min(maxWaitMillis - elapsedTimeMillis, currentSleepTime);
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

	/**
	 * This utility method tries to connect to the JobManager using the InetAddress returned by
	 * InetAddress.getLocalHost(). The purpose of the utility is to have a final try connecting to
	 * the target address using the LocalHost before using the address returned.
	 * We do a second try because the JM might have been unavailable during the first check.
	 *
	 * @param preliminaryResult The address detected by the heuristic
	 * @return either the preliminaryResult or the address returned by InetAddress.getLocalHost() (if
	 * 			we are able to connect to targetAddress from there)
	 */
	private static InetAddress tryLocalHostBeforeReturning(
				InetAddress preliminaryResult, SocketAddress targetAddress, boolean logging) throws IOException {

		InetAddress localhostName = InetAddress.getLocalHost();

		if (preliminaryResult.equals(localhostName)) {
			// preliminary result is equal to the local host name
			return preliminaryResult;
		}
		else if (tryToConnect(localhostName, targetAddress, AddressDetectionState.SLOW_CONNECT.getTimeout(), logging)) {
			// success, we were able to use local host to connect
			LOG.debug("Preferring {} (InetAddress.getLocalHost()) for local bind point over previous candidate {}",
					localhostName, preliminaryResult);
			return localhostName;
		}
		else {
			// we have to make the preliminary result the final result
			return preliminaryResult;
		}
	}

	/**
	 * Try to find a local address which allows as to connect to the targetAddress using the given
	 * strategy.
	 *
	 * @param strategy Depending on the strategy, the method will enumerate all interfaces, trying to connect
	 *                 to the target address
	 * @param targetAddress The address we try to connect to
	 * @param logging Boolean indicating the logging verbosity
	 * @return null if we could not find an address using this strategy, otherwise, the local address.
	 * @throws IOException
	 */
	private static InetAddress findAddressUsingStrategy(AddressDetectionState strategy,
														InetSocketAddress targetAddress,
														boolean logging) throws IOException {
		// try LOCAL_HOST strategy independent of the network interfaces
		if (strategy == AddressDetectionState.LOCAL_HOST) {
			InetAddress localhostName;
			try {
				localhostName = InetAddress.getLocalHost();
			} catch (UnknownHostException uhe) {
				LOG.warn("Could not resolve local hostname to an IP address: {}", uhe.getMessage());
				return null;
			}

			if (tryToConnect(localhostName, targetAddress, strategy.getTimeout(), logging)) {
				LOG.debug("Using InetAddress.getLocalHost() immediately for the connecting address");
				// Here, we are not calling tryLocalHostBeforeReturning() because it is the LOCAL_HOST strategy
				return localhostName;
			} else {
				return null;
			}
		}

		final InetAddress address = targetAddress.getAddress();
		if (address == null) {
			return null;
		}
		final byte[] targetAddressBytes = address.getAddress();

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
								return tryLocalHostBeforeReturning(interfaceAddress, targetAddress, logging);
							}
						}
						break;

					case FAST_CONNECT:
					case SLOW_CONNECT:
						LOG.debug("Trying to connect to {} from local address {} with timeout {}",
								targetAddress, interfaceAddress, strategy.getTimeout());

						if (tryToConnect(interfaceAddress, targetAddress, strategy.getTimeout(), logging)) {
							return tryLocalHostBeforeReturning(interfaceAddress, targetAddress, logging);
						}
						break;

					case HEURISTIC:
						if (LOG.isDebugEnabled()) {
							LOG.debug("Choosing InetAddress.getLocalHost() address as a heuristic.");
						}

						return InetAddress.getLocalHost();

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
										int timeout, boolean logFailed) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to connect to (" + toSocket + ") from local address " + fromAddress
					+ " with timeout " + timeout);
		}
		try (Socket socket = new Socket()) {
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
	}

	/**
	 * A {@link LeaderRetrievalListener} that allows retrieving an {@link InetAddress} for the current leader.
	 */
	public static class LeaderConnectingAddressListener implements LeaderRetrievalListener {

		private static final Duration defaultLoggingDelay = Duration.ofMillis(400);

		private enum LeaderRetrievalState {
			NOT_RETRIEVED,
			RETRIEVED,
			NEWLY_RETRIEVED
		}

		private final Object retrievalLock = new Object();

		private String akkaURL;
		private LeaderRetrievalState retrievalState = LeaderRetrievalState.NOT_RETRIEVED;
		private Exception exception;

		public InetAddress findConnectingAddress(
				Duration timeout) throws LeaderRetrievalException {
			return findConnectingAddress(timeout, defaultLoggingDelay);
		}

		public InetAddress findConnectingAddress(
				Duration timeout,
				Duration startLoggingAfter) throws LeaderRetrievalException {

			final long startTimeNanos = System.nanoTime();
			long currentSleepTime = MIN_SLEEP_TIME;
			long elapsedTimeMillis = 0;
			InetSocketAddress targetAddress = null;

			try {
				while (elapsedTimeMillis < timeout.toMillis()) {

					long maxTimeout = timeout.toMillis() - elapsedTimeMillis;

					synchronized (retrievalLock) {
						if (exception != null) {
							throw exception;
						}

						if (retrievalState == LeaderRetrievalState.NOT_RETRIEVED) {
							try {
								retrievalLock.wait(maxTimeout);
							} catch (InterruptedException e) {
								throw new Exception("Finding connecting address was interrupted" +
										"while waiting for the leader retrieval.");
							}
						} else if (retrievalState == LeaderRetrievalState.NEWLY_RETRIEVED) {
							targetAddress = AkkaUtils.getInetSocketAddressFromAkkaURL(akkaURL);

							LOG.info("Retrieved new target address {}.", targetAddress);

							retrievalState = LeaderRetrievalState.RETRIEVED;

							currentSleepTime = MIN_SLEEP_TIME;
						} else {
							currentSleepTime = Math.min(2 * currentSleepTime, MAX_SLEEP_TIME);
						}
					}

					if (targetAddress != null) {
						AddressDetectionState strategy = AddressDetectionState.LOCAL_HOST;

						boolean logging = elapsedTimeMillis >= startLoggingAfter.toMillis();
						if (logging) {
							LOG.info("Trying to connect to address {}", targetAddress);
						}

						do {
							InetAddress address = ConnectionUtils.findAddressUsingStrategy(strategy, targetAddress, logging);
							if (address != null) {
								return address;
							}

							// pick the next strategy
							switch (strategy) {
								case LOCAL_HOST:
									strategy = AddressDetectionState.ADDRESS;
									break;
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
					}

					elapsedTimeMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;

					long timeToWait = Math.min(
							Math.max(timeout.toMillis() - elapsedTimeMillis, 0),
							currentSleepTime);

					if (timeToWait > 0) {
						synchronized (retrievalLock) {
							try {
								retrievalLock.wait(timeToWait);
							} catch (InterruptedException e) {
								throw new Exception("Finding connecting address was interrupted while pausing.");
							}
						}

						elapsedTimeMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;
					}
				}

				InetAddress heuristic = null;

				if (targetAddress != null) {
					LOG.warn("Could not connect to {}. Selecting a local address using heuristics.", targetAddress);
					heuristic = findAddressUsingStrategy(AddressDetectionState.HEURISTIC, targetAddress, true);
				}

				if (heuristic != null) {
					return heuristic;
				} else {
					LOG.warn("Could not find any IPv4 address that is not loopback or link-local. Using localhost address.");
					return InetAddress.getLocalHost();
				}
			} catch (Exception e) {
				throw new LeaderRetrievalException("Could not retrieve the connecting address to the " +
						"current leader with the akka URL " + akkaURL + ".", e);
			}
		}

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			if (leaderAddress != null && !leaderAddress.isEmpty()) {
				synchronized (retrievalLock) {
					akkaURL = leaderAddress;
					retrievalState = LeaderRetrievalState.NEWLY_RETRIEVED;

					retrievalLock.notifyAll();
				}
			}
		}

		@Override
		public void handleError(Exception exception) {
			synchronized (retrievalLock) {
				this.exception = exception;
				retrievalLock.notifyAll();
			}
		}
	}
}
