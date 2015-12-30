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

import com.google.common.collect.Iterators;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NetUtils {

	private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);
	
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

	// ------------------------------------------------------------------------
	//  Lookup of to free ports
	// ------------------------------------------------------------------------
	
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
	

	// ------------------------------------------------------------------------
	//  Encoding of IP addresses for URLs
	// ------------------------------------------------------------------------
	
	/**
	 * Encodes an IP address properly as a URL string. This method makes sure that IPv6 addresses
	 * have the proper formatting to be included in URLs.
	 * <p>
	 * This method internally uses Guava's functionality to properly encode IPv6 addresses.
	 * 
	 * @param address The IP address to encode.
	 * @return The proper URL string encoded IP address.
	 */
	public static String ipAddressToUrlString(InetAddress address) {
		if (address == null) {
			throw new NullPointerException("address is null");
		}
		else if (address instanceof Inet4Address) {
			return address.getHostAddress();
		}
		else if (address instanceof Inet6Address) {
			return '[' + InetAddresses.toAddrString(address) + ']';
		}
		else {
			throw new IllegalArgumentException("Unrecognized type of InetAddress: " + address);
		}
	}

	/**
	 * Encodes an IP address and port to be included in URL. in particular, this method makes
	 * sure that IPv6 addresses have the proper formatting to be included in URLs.
	 * 
	 * @param address The address to be included in the URL.
	 * @param port The port for the URL address.
	 * @return The proper URL string encoded IP address and port.
	 */
	public static String ipAddressAndPortToUrlString(InetAddress address, int port) {
		return ipAddressToUrlString(address) + ':' + port;
	}

	/**
	 * Encodes an IP address and port to be included in URL. in particular, this method makes
	 * sure that IPv6 addresses have the proper formatting to be included in URLs.
	 * 
	 * @param address The socket address with the IP address and port.
	 * @return The proper URL string encoded IP address and port.
	 */
	public static String socketAddressToUrlString(InetSocketAddress address) {
		if (address.isUnresolved()) {
			throw new IllegalArgumentException("Address cannot be resolved: " + address.getHostString());
		}
		return ipAddressAndPortToUrlString(address.getAddress(), address.getPort());
	}

	/**
	 * Normalizes and encodes a hostname and port to be included in URL. 
	 * In particular, this method makes sure that IPv6 address literals have the proper
	 * formatting to be included in URLs.
	 *
	 * @param host The address to be included in the URL.
	 * @param port The port for the URL address.
	 * @return The proper URL string encoded IP address and port.
	 * @throws java.net.UnknownHostException Thrown, if the hostname cannot be translated into a URL.
	 */
	public static String hostAndPortToUrlString(String host, int port) throws UnknownHostException {
		return ipAddressAndPortToUrlString(InetAddress.getByName(host), port);
	}

	/**
	 * Returns an iterator over available ports defined by the range definition.
	 *
	 * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.
	 * @return Set of ports from the range definition
	 * @throws NumberFormatException If an invalid string is passed.
	 */

	public static Iterator<Integer> getPortRangeFromString(String rangeDefinition) throws NumberFormatException {
		final String[] ranges = rangeDefinition.trim().split(",");
		List<Iterator<Integer>> iterators = new ArrayList<>(ranges.length);
		for(String rawRange: ranges) {
			Iterator<Integer> rangeIterator = null;
			String range = rawRange.trim();
			int dashIdx = range.indexOf('-');
			if (dashIdx == -1) {
				// only one port in range:
				rangeIterator = Iterators.singletonIterator(Integer.valueOf(range));
			} else {
				// evaluate range
				final int start = Integer.valueOf(range.substring(0, dashIdx));
				final int end = Integer.valueOf(range.substring(dashIdx+1, range.length()));
				rangeIterator = new Iterator<Integer>() {
					int i = start;
					@Override
					public boolean hasNext() {
						return i <= end;
					}

					@Override
					public Integer next() {
						return i++;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException("Remove not supported");
					}
				};
			}
			iterators.add(rangeIterator);
		}
		return Iterators.concat(iterators.iterator());
	}

	/**
	 * Tries to allocate a socket from the given sets of ports.
	 *
	 * @param portsIterator A set of ports to choose from.
	 * @param factory A factory for creating the SocketServer
	 * @return null if no port was available or an allocated socket.
	 */
	public static ServerSocket createSocketFromPorts(Iterator<Integer> portsIterator, SocketFactory factory) throws IOException {
		while (portsIterator.hasNext()) {
			int port = portsIterator.next();
			LOG.debug("Trying to open socket on port {}", port);
			try {
				return factory.createSocket(port);
			} catch (IOException | IllegalArgumentException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unable to allocate socket on port", e);
				} else {
					LOG.info("Unable to allocate on port {}, due to error: {}", port, e.getMessage());
				}
			}
		}
		return null;
	}

	public interface SocketFactory {
		ServerSocket createSocket(int port) throws IOException;
	}
}
