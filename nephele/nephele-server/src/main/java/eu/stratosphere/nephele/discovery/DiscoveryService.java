/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The discovery service allows task managers to discover a job manager
 * through an IPv4 broadcast or IPv6 multicast. The service has two components:
 * A server component that runs at the job manager and listens for incoming
 * requests and a client component a task manager can use to issue discovery
 * requests.
 * <p>
 * The discovery service uses the <code>discoveryservice.magicnumber</code> configuration parameter. It needs to be set
 * to any number. Task managers discover the job manager only if their magic number matches. This allows running two
 * Nephele setups on the same cluster without interference of the {@link DiscoveryService}s.
 * <p>
 * On hosts with several network interfaces or IP addresses, the <code>servicenetwork</code> can be used to describe to
 * which IP the services shall be bound. A node with IP addresses 130.149.3.99/255.255.255.192 and
 * 192.168.198.3/255.255.0.0 could specify for example 130.149.3.64 or 192.168.0.0 as the service network. In fact also
 * 130.149.3.99 and 192.168.198.3 would work.
 * 
 * @author warneke
 * @author Dominic Battre
 */
public class DiscoveryService implements Runnable {

	/**
	 * Network port of the discovery listens on for incoming connections.
	 */
	private static final int DISCOVERYPORT = 7001;

	/**
	 * Number of retries before discovery is considered to be failed.
	 */
	private static final int DISCOVERFAILURERETRIES = 10;

	/**
	 * Timeout (in msec) for the client socket.
	 */
	private static final int CLIENTSOCKETTIMEOUT = 1000;

	/**
	 * The IPv6 multicast address for link-local all-nodes.
	 */
	private static final String IPV6MULTICASTADDRESS = "FF02::1";

	/**
	 * The key to retrieve the discovery service's magic number from the configuration.
	 */
	private static final String MAGICNUMBER_KEY = "discoveryservice.magicnumber";

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(DiscoveryService.class);

	/**
	 * Singleton instance of the discovery service.
	 */
	private static DiscoveryService discoveryService = null;

	/**
	 * The network port that is announced for the job manager's IPC service.
	 */
	private final int ipcPort;

	/**
	 * The thread executing the receive operation on the discovery port.
	 */
	private Thread listeningThread = null;

	private final static Pattern pingPattern = Pattern.compile("PING (\\d+)");

	private final static Pattern pongPattern = Pattern.compile("PONG (\\d+)");

	/**
	 * The datagram socket of the discovery server.
	 */
	private DatagramSocket serverSocket = null;

	private static final boolean USEIPV6 = "true".equals(System.getProperty("java.net.preferIPv4Stack")) ? false : true;

	/**
	 * Constructs a new {@link DiscoveryService} object and stores
	 * the job manager's IPC port.
	 * 
	 * @param ipcPort
	 */
	private DiscoveryService(int ipcPort) {
		this.ipcPort = ipcPort;
	}

	/**
	 * Starts a new discovery service.
	 * 
	 * @param ipcPort
	 *        the network port that is announced for
	 *        the job manager's IPC service.
	 * @throws DiscoveryException
	 *         thrown if the discovery service could not be started because
	 *         of network difficulties
	 */
	public static void startDiscoveryService(int ipcPort) throws DiscoveryException {

		if (discoveryService == null) {
			discoveryService = new DiscoveryService(ipcPort);
			discoveryService.startService();
		}
	}

	/**
	 * Stops the discovery service.
	 */
	public static void stopDiscoveryService() {

		if (discoveryService != null) {
			discoveryService.stopService();
		}

	}

	/**
	 * Auxiliary method to start the discovery service.
	 * 
	 * @throws DiscoveryException
	 *         thrown if the discovery service could not be started because
	 *         of network difficulties
	 */
	private void startService() throws DiscoveryException {

		try {
			this.serverSocket = new DatagramSocket(DISCOVERYPORT, getServiceAddress());
		} catch (SocketException e) {
			throw new DiscoveryException(e.toString());
		}

		LOG.debug("Discovery service socket is bound to " + this.serverSocket.getLocalSocketAddress());

		this.listeningThread = new Thread(this);
		this.listeningThread.start();
	}

	/**
	 * Auxiliary method to stop the discovery service.
	 */
	private void stopService() {

		LOG.debug("Stopping discovery service on port" + DISCOVERYPORT);

		this.listeningThread.interrupt();

		// Close the server socket
		this.serverSocket.close();
	}

	/**
	 * Creates a new PING Message.
	 * <p>
	 * The message follows the format "PING X", where X is a magic number configured in "discoveryservice.magicnumber"
	 * or 0 if no number is specified in the configuration. The magic number allows to execute several Nephele instances
	 * in the same network without IP traffic isolation. Without such a mechanism, one Task Manager might register at
	 * the wrong Discovery Service.
	 * 
	 * @return new PING datagram.
	 */
	private static DatagramPacket createPingPacket() {
		int magicNumber = GlobalConfiguration.getInteger(MAGICNUMBER_KEY, 0);
		byte[] bytes = ("PING " + magicNumber).getBytes();
		return new DatagramPacket(bytes, bytes.length);
	}

	/**
	 * Returns whether the {@link DatagramPacket} contains a PING Message
	 * that is addressed to us.
	 * 
	 * @see {@link #createPingPacket()} for an explanation of the message format
	 * @param packet
	 *        Received {@link DatagramPacket} that might contain a PING message
	 * @return true if the {@link DatagramPacket} contains a PING message addressed to us.
	 */
	private static boolean isPingForUs(DatagramPacket packet) {

		try {
			String content = new String(packet.getData(), packet.getOffset(), packet.getLength());

			Matcher m = pingPattern.matcher(content);

			if (m.matches()) {
				final int magicNumber = GlobalConfiguration.getInteger(MAGICNUMBER_KEY, 0);
				if (Integer.parseInt(m.group(1)) == magicNumber) {
					return true;
				}
			}
		} catch (Exception e) {
			LOG.error("Error parsing ping", e);
		}
		return false;
	}

	/**
	 * Attempts to retrieve the job managers address in the network through an
	 * IP broadcast. This method should be called by the task manager.
	 * 
	 * @return the socket address of the job manager in the network
	 * @throws DiscoveryException
	 *         thrown if the job manager's socket address could not be
	 *         discovered
	 */
	public static InetSocketAddress getJobManagerAddress() throws DiscoveryException {

		/*
		 * try {
		 * InetSocketAddress result = new InetSocketAddress(InetAddress.getByName("192.168.2.111"),6123);
		 * return result;
		 * } catch (UnknownHostException e) {
		 * // TODO Auto-generated catch block
		 * e.printStackTrace();
		 * throw new DiscoveryException("Unable toooo discoer JobManager via IP broadcast!");
		 * }
		 */
		InetSocketAddress jobManagerAddress = null;

		DatagramSocket socket = null;

		try {

			final Set<InetAddress> targetAddresses = getBroadcastAddresses();

			if (targetAddresses.isEmpty()) {
				throw new DiscoveryException("Could not find any broadcast addresses available to this host");
			}

			socket = new DatagramSocket();

			LOG.debug("Setting socket timeout to " + CLIENTSOCKETTIMEOUT);
			socket.setSoTimeout(CLIENTSOCKETTIMEOUT);

			final DatagramPacket pongBuffer = new DatagramPacket(new byte[100], 100);

			for (int retries = 0; retries < DISCOVERFAILURERETRIES; retries++) {

				for (InetAddress broadcast : targetAddresses) {
					final DatagramPacket ping = createPingPacket();
					ping.setAddress(broadcast);
					ping.setPort(DISCOVERYPORT);
					LOG.debug("Sending discovery request to " + ping.getSocketAddress());
					socket.send(ping);
				}

				try {
					socket.receive(pongBuffer);
				} catch (SocketTimeoutException ste) {
					LOG.debug("Timeout wainting for discovery reply. Retrying...");
					continue;
				}

				final int ipcPort = extractIpcPort(pongBuffer);
				// TODO: This condition helps to deal with legacy implementations of the DiscoveryService
				if (ipcPort < 0) {
					continue;
				}

				// Replace port from discovery service with the actual RPC port
				// of the job manager
				if (USEIPV6) {
					// TODO: No connection possible unless we remove the scope identifier
					if (pongBuffer.getAddress() instanceof Inet6Address) {
						try {
							jobManagerAddress = new InetSocketAddress(InetAddress.getByAddress(pongBuffer.getAddress()
								.getAddress()), ipcPort);
						} catch (UnknownHostException e) {
							throw new DiscoveryException(StringUtils.stringifyException(e));
						}
					} else {
						throw new DiscoveryException(pongBuffer.getAddress() + " is not a valid IPv6 address");
					}
				} else {
					jobManagerAddress = new InetSocketAddress(pongBuffer.getAddress(), ipcPort);
				}
				LOG.debug("Discovered job manager at " + jobManagerAddress);
				break;
			}

		} catch (IOException ioe) {
			throw new DiscoveryException(ioe.toString());
		} finally {
			if (socket != null) {
				socket.close();
			}
		}

		if (jobManagerAddress == null) {
			LOG.debug("Unable to discover Jobmanager via IP broadcast");
			throw new DiscoveryException("Unable to discover JobManager via IP broadcast!");
		}

		return jobManagerAddress;
	}

	private static int extractIpcPort(DatagramPacket packet) {

		final String content = new String(packet.getData(), packet.getOffset(), packet.getLength());

		Matcher m = pongPattern.matcher(content);

		if (!m.matches()) {
			LOG.error("DiscoveryService cannot extract port from " + content);
			return -1;
		}

		LOG.debug("Received response from DiscoveryService: " + content);

		int ipcPort = 0;

		try {
			ipcPort = Integer.parseInt(m.group(1));
		} catch (NumberFormatException e) {
			LOG.error(StringUtils.stringifyException(e));
			return -1;
		}

		return ipcPort;
	}

	/**
	 * Returns the set of broadcast addresses available to the network
	 * interfaces of this host. In case of IPv6 the set contains the
	 * IPv6 multicast address to reach all nodes on the local link.
	 * 
	 * @return (possibly empty) set of broadcast addresses reachable by this host
	 */
	private static Set<InetAddress> getBroadcastAddresses() {

		final Set<InetAddress> broadcastAddresses = new HashSet<InetAddress>();

		// get all network interfaces
		Enumeration<NetworkInterface> ie = null;
		try {
			ie = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e) {
			LOG.error("Could not collect network interfaces of host", e);
			return broadcastAddresses;
		}

		final InetAddress serviceAddress = getServiceAddress();

		while (ie.hasMoreElements()) {
			NetworkInterface nic = ie.nextElement();
			try {
				if (!nic.isUp()) {
					continue;
				}

				// check all IPs bound to network interfaces
				for (InterfaceAddress adr : nic.getInterfaceAddresses()) {

					// collect all broadcast addresses
					if (USEIPV6) {
						try {
							final InetAddress interfaceAddress = adr.getAddress();
							if (interfaceAddress instanceof Inet6Address) {
								final Inet6Address ipv6Address = (Inet6Address) interfaceAddress;
								final InetAddress multicastAddress = InetAddress.getByName(IPV6MULTICASTADDRESS + "%"
									+ Integer.toString(ipv6Address.getScopeId()));
								broadcastAddresses.add(multicastAddress);
							}

						} catch (UnknownHostException e) {
							LOG.error(e);
						}
					} else {
						final InetAddress broadcast = adr.getBroadcast();
						if ((broadcast != null && serviceAddress == null)
							|| (broadcast != null && DiscoveryService.onSameNetwork(serviceAddress, broadcast, adr
								.getNetworkPrefixLength()))) {
							broadcastAddresses.add(broadcast);
						}
					}
				}

			} catch (SocketException e) {
				LOG.error("Socket exception when checking " + nic.getName() + ". " + "Ignoring this device.", e);
			}
		}

		return broadcastAddresses;
	}

	/**
	 * Calculates the bit vector of the network prefix for a specific <code>addressLength</code> and
	 * <code>networkPrefixLength</code>
	 * 
	 * @param addressLength
	 *        the length (in bits) of the network address
	 * @param networkPrefixLength
	 *        the length (in bits) of network address prefix
	 * @return bit vector representing the prefix of the network address
	 */
	@VisibleForTesting
	static byte[] getNetworkPrefix(int addressLength, int networkPrefixLength) {

		if (networkPrefixLength <= 0 || networkPrefixLength >= addressLength) {
			throw new IllegalArgumentException("Invalid networkPrefixLength");
		}

		byte[] netmask = new byte[addressLength / 8];

		for (int byteNr = 0; byteNr < netmask.length; ++byteNr) {
			// create netmask for the current byte

			// how many '1's remain for this byte?
			// e.g. if networkPrefixLength was 11,
			// then we have 8x'1' in the first byte
			// and 3x'1' followed by 5x'0' in the second byte
			int onesInThisByte = Math.min(networkPrefixLength, 8);
			networkPrefixLength -= onesInThisByte;

			// calculate bit pattern for current byte.

			// suppose onesInThisByte is 3
			// (1<<5) = 0010 0000
			// (1<<5)-1 = 0001 1111
			// ~(1<<5)-1) = 1110 0000

			netmask[byteNr] = (byte) ~((1 << (8 - onesInThisByte)) - 1);
		}

		return netmask;
	}

	/**
	 * Returns for a given <code>networkPrefixLength</code> whether <code>a</code> and <code>b</code> are in the same
	 * network.
	 * 
	 * @param a
	 *        first IP Address
	 * @param b
	 *        second IP Address
	 * @param networkPrefixLength
	 *        number of bits in IP addresses belonging to network id
	 * @return true if a and b belong to the same network.
	 */
	@VisibleForTesting
	static boolean onSameNetwork(InetAddress a, InetAddress b, int networkPrefixLength) {

		if ((a == null) || (b == null)) {
			return false;
		}

		// convert both addresses to byte array
		byte[] A = a.getAddress();
		byte[] B = b.getAddress();

		// Compatible addresses must have the same length
		if (A.length != B.length) {
			return false;
		}

		byte[] prefix = getNetworkPrefix(A.length * 8, networkPrefixLength);

		// check byte wise whether (A & netmask) = (B & netmask).
		for (int byteNr = 0; byteNr < A.length; ++byteNr) {
			if ((A[byteNr] & prefix[byteNr]) != (B[byteNr] & prefix[byteNr])) {
				return false;
			}
		}
		return true;
	}

	/**
	 * This function returns the IP address to which services shall bind.
	 * <p>
	 * If the configuration file contains an entry like <code>servicenetwork=192.168.178.0</code> (or a respective IPv6
	 * address), this function returns the first IP bound to a network interface that belongs to the same network as the
	 * specified <code>servicenetwork</code>.
	 * <p>
	 * If the configuration file does not contain a <code>servicenetwork</code> entry, the function returns
	 * <code>null</code>. As a result, services will be bound to any/all local addresses. If no valid IP address can be
	 * found, a {@link SocketException} is thrown.
	 * 
	 * @return {@link InetAddress} to which services shall bind.
	 */
	public static InetAddress getServiceAddress() {

		final String serviceNetwork = GlobalConfiguration.getString("servicenetwork", null);

		InetAddress serviceNetworkAddress = null;

		if (serviceNetwork == null) {
			return null;
		}

		try {
			serviceNetworkAddress = InetAddress.getByName(serviceNetwork);

			if ((serviceNetworkAddress instanceof Inet4Address) && USEIPV6) {
				throw new UnknownHostException();
			}

			if ((serviceNetworkAddress instanceof Inet6Address) && !USEIPV6) {
				throw new UnknownHostException();
			}

		} catch (UnknownHostException e) {
			if (USEIPV6) {
				LOG.error("Configured service network is not a valid IPv6 address");
			} else {
				LOG.error("Configured service network is not a valid IPv4 address");
			}
		}

		// If the service network address could not be parsed, we fall back to all interfaces
		if (serviceNetworkAddress == null) {
			return null;
		}

		return findLocalAddressOnSameNetwork(serviceNetworkAddress);
	}

	/**
	 * Server side implementation of Discovery Service.
	 */
	@Override
	public void run() {

		final DatagramPacket ping = new DatagramPacket(new byte[100], 100);
		final byte[] PONG = ("PONG " + Integer.toString(this.ipcPort)).getBytes();
		final DatagramPacket pong = new DatagramPacket(PONG, PONG.length);

		while (!Thread.interrupted()) {

			try {
				this.serverSocket.receive(ping);

				if (isPingForUs(ping)) {
					LOG.debug("Received ping from " + ping.getSocketAddress());
					pong.setAddress(ping.getAddress());
					pong.setPort(ping.getPort());

					this.serverSocket.send(pong);
				} else {
					LOG.debug("Received ping for somebody else from " + ping.getSocketAddress());
				}
			} catch (SocketTimeoutException ste) {
				LOG.debug("Discovery service: socket timeout");
			} catch (IOException ioe) {
				LOG.error("Discovery service stopped working with IOException:\n" + ioe.toString());
				break;
			}
		}

		// Close the socket finally
		this.serverSocket.close();
	}

	/**
	 * Finds a local network address that is on the same network as <code>candidateAddress</code> or <code>null</code>
	 * if no such
	 * address is attached to one of the host's interfaces.
	 * 
	 * @param candidateAddress
	 *        the candidate address
	 * @return a local network address on the same network as <code>candidateAddress</code> or <code>null</code> if no
	 *         such address can be found
	 */
	public static InetAddress findLocalAddressOnSameNetwork(InetAddress candidateAddress) {

		if (candidateAddress == null) {
			LOG.debug("candidateAddress is null");
			return null;
		}

		// If candidate address is a loopback address, simply return the address
		if (candidateAddress.isLoopbackAddress()) {
			return candidateAddress;
		}

		// iterate over all network interfaces and return first address that
		// is in the same network as candidateAddress
		try {
			Enumeration<NetworkInterface> ie = NetworkInterface.getNetworkInterfaces();

			while (ie.hasMoreElements()) {
				NetworkInterface i = ie.nextElement();
				if (!i.isUp()) {
					continue;
				}

				for (InterfaceAddress adr : i.getInterfaceAddresses()) {
					InetAddress address = adr.getAddress();
					if ((address instanceof Inet4Address) && USEIPV6) {
						continue;
					}

					if ((address instanceof Inet6Address) && !USEIPV6) {
						continue;
					}

					final int networkPrefixLength = adr.getNetworkPrefixLength();

					if (i.isPointToPoint() == false && onSameNetwork(address, candidateAddress, networkPrefixLength)) {
						return address;
					}
				}
			}
		} catch (SocketException e) {
			LOG.error(e);
		}

		return null;
	}
}
