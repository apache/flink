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
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 * 
 * @author warneke
 * @author Dominic Battre
 */
public class DiscoveryService implements Runnable {

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
	 * The default magic number.
	 */
	private static final int DEFAULT_MAGICNUMBER = 0;

	/**
	 * The key to retrieve the network port the discovery service listens on for incoming connections from the
	 * configuration.
	 */
	private static final String DISCOVERYPORT_KEY = "discoveryservice.port";

	/**
	 * The default network port the discovery service listens on for incoming connections.
	 */
	private static final int DEFAULT_DISCOVERYPORT = 7001;

	/**
	 * Flag indicating whether to use IPv6 or not.
	 */
	private static final boolean USE_IPV6 = "true".equals(System.getProperty("java.net.preferIPv4Stack")) ? false
		: true;

	/**
	 * ID for job manager lookup request packets.
	 */
	private static final int JM_LOOKUP_REQUEST_ID = 0;

	/**
	 * ID for job manager lookup reply packets.
	 */
	private static final int JM_LOOKUP_REPLY_ID = 1;

	/**
	 * ID for task manager address request packets.
	 */
	private static final int TM_ADDRESS_REQUEST_ID = 2;

	/**
	 * ID for task manager address reply packets.
	 */
	private static final int TM_ADDRESS_REPLY_ID = 3;

	/**
	 * The default size of response datagram packets.
	 */
	private static final int RESPONSE_PACKET_SIZE = 64;

	/**
	 * The offset inside a packet to the magic number field.
	 */
	private static final int MAGIC_NUMBER_OFFSET = 0;

	/**
	 * The offset inside a packet to the packet ID field.
	 */
	private static final int PACKET_ID_OFFSET = 4;

	/**
	 * The offset inside a packet to the packet type ID field.
	 */
	private static final int PACKET_TYPE_ID_OFFSET = 8;

	/**
	 * The offset inside a packet to the actual payload.
	 */
	private static final int PAYLOAD_OFFSET = 12;

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(DiscoveryService.class);

	/**
	 * Singleton instance of the discovery service.
	 */
	private static DiscoveryService discoveryService = null;

	/**
	 * The network port the discovery service listens on for incoming connections.
	 */
	private final int discoveryPort;

	/**
	 * The magic number used to identify this instance of the discovery service.
	 */
	private final int magicNumber;

	/**
	 * The network address the IPC is bound to, possibly <code>null</code>.
	 */
	private final InetAddress ipcAddress;

	/**
	 * The network port that is announced for the job manager's IPC service.
	 */
	private final int ipcPort;

	/**
	 * The thread executing the receive operation on the discovery port.
	 */
	private Thread listeningThread = null;

	/**
	 * The datagram socket of the discovery server.
	 */
	private DatagramSocket serverSocket = null;

	/**
	 * Flag to check whether the service is running
	 */
	private volatile boolean isRunning = false;

	/**
	 * Constructs a new {@link DiscoveryService} object and stores
	 * the job manager's IPC port.
	 * 
	 * @param ipcAddress
	 *        the network address the IPC is bound to, possibly <code>null</code>
	 * @param ipcPort
	 *        the network port that is announced for the job manager's IPC service
	 */
	private DiscoveryService(final InetAddress ipcAddress, final int ipcPort) {

		this.discoveryPort = GlobalConfiguration.getInteger(DISCOVERYPORT_KEY, DEFAULT_DISCOVERYPORT);
		this.magicNumber = GlobalConfiguration.getInteger(MAGICNUMBER_KEY, DEFAULT_MAGICNUMBER);

		this.ipcAddress = ipcAddress;
		this.ipcPort = ipcPort;
	}

	/**
	 * Starts a new discovery service.
	 * 
	 * @param ipcAddress
	 *        the network address the IPC is bound to, possibly <code>null</code>
	 * @param ipcPort
	 *        the network port that is announced for the job manager's IPC service.
	 * @throws DiscoveryException
	 *         thrown if the discovery service could not be started because
	 *         of network difficulties
	 */
	public static synchronized void startDiscoveryService(final InetAddress ipcAddress, final int ipcPort)
			throws DiscoveryException {

		if (discoveryService == null) {
			discoveryService = new DiscoveryService(ipcAddress, ipcPort);
		}

		if (!discoveryService.isRunning()) {
			discoveryService.startService();
		}
	}

	/**
	 * Checks whether the discovery service is running.
	 * 
	 * @return <code>true</code> if the service is running, <code>false</code> otherwise
	 */
	public boolean isRunning() {

		return this.isRunning;
	}

	/**
	 * Stops the discovery service.
	 */
	public static synchronized void stopDiscoveryService() {

		if (discoveryService != null) {
			if (discoveryService.isRunning()) {
				discoveryService.stopService();
			}
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
			this.serverSocket = new DatagramSocket(this.discoveryPort, this.ipcAddress);
		} catch (SocketException e) {
			throw new DiscoveryException(e.toString());
		}

		LOG.info("Discovery service socket is bound to " + this.serverSocket.getLocalSocketAddress());

		this.isRunning = true;

		this.listeningThread = new Thread(this);
		this.listeningThread.start();
	}

	/**
	 * Auxiliary method to stop the discovery service.
	 */
	private void stopService() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping discovery service on port" + this.discoveryPort);
		}

		this.isRunning = false;

		this.listeningThread.interrupt();

		// Close the server socket
		this.serverSocket.close();
	}

	/**
	 * Creates a new job manager lookup request packet.
	 * 
	 * @param magicNumber
	 *        the magic number to identify this discovery service
	 * @return a new job manager lookup request packet
	 */
	private static DatagramPacket createJobManagerLookupRequestPacket(final int magicNumber) {

		final byte[] bytes = new byte[12];
		integerToByteArray(magicNumber, MAGIC_NUMBER_OFFSET, bytes);
		integerToByteArray(generateRandomPacketID(), PACKET_ID_OFFSET, bytes);
		integerToByteArray(JM_LOOKUP_REQUEST_ID, PACKET_TYPE_ID_OFFSET, bytes);

		return new DatagramPacket(bytes, bytes.length);
	}

	/**
	 * Creates a new job manager lookup reply packet.
	 * 
	 * @param ipcPort
	 *        the port of the job manager's IPC server
	 * @param magicNumber
	 *        the magic number to identify this discovery service
	 * @return a new job manager lookup reply packet
	 */
	private static DatagramPacket createJobManagerLookupReplyPacket(final int ipcPort, final int magicNumber) {

		final byte[] bytes = new byte[16];
		integerToByteArray(magicNumber, MAGIC_NUMBER_OFFSET, bytes);
		integerToByteArray(generateRandomPacketID(), PACKET_ID_OFFSET, bytes);
		integerToByteArray(JM_LOOKUP_REPLY_ID, PACKET_TYPE_ID_OFFSET, bytes);
		integerToByteArray(ipcPort, PAYLOAD_OFFSET, bytes);

		return new DatagramPacket(bytes, bytes.length);
	}

	/**
	 * Creates a new task manager address request packet.
	 * 
	 * @param magicNumber
	 *        the magic number to identify this discovery service
	 * @return a new task manager address request packet
	 */
	private static DatagramPacket createTaskManagerAddressRequestPacket(final int magicNumber) {

		final byte[] bytes = new byte[12];
		integerToByteArray(magicNumber, MAGIC_NUMBER_OFFSET, bytes);
		integerToByteArray(generateRandomPacketID(), PACKET_ID_OFFSET, bytes);
		integerToByteArray(TM_ADDRESS_REQUEST_ID, PACKET_TYPE_ID_OFFSET, bytes);

		return new DatagramPacket(bytes, bytes.length);
	}

	/**
	 * Creates a new task manager address reply packet.
	 * 
	 * @param taskManagerAddress
	 *        the address of the task manager which sent the request
	 * @param magicNumber
	 *        the magic number to identify this discovery service
	 * @return a new task manager address reply packet
	 */
	private static DatagramPacket createTaskManagerAddressReplyPacket(final InetAddress taskManagerAddress,
			final int magicNumber) {

		final byte[] addr = taskManagerAddress.getAddress();
		final byte[] bytes = new byte[20 + addr.length];
		integerToByteArray(magicNumber, MAGIC_NUMBER_OFFSET, bytes);
		integerToByteArray(generateRandomPacketID(), PACKET_ID_OFFSET, bytes);
		integerToByteArray(TM_ADDRESS_REPLY_ID, PACKET_TYPE_ID_OFFSET, bytes);
		integerToByteArray(addr.length, PAYLOAD_OFFSET, bytes);
		System.arraycopy(addr, 0, bytes, PAYLOAD_OFFSET + 4, addr.length);

		return new DatagramPacket(bytes, bytes.length);
	}

	/**
	 * Returns the network address with which the task manager shall announce itself to the job manager. To determine
	 * the address this method exchanges packets with the job manager.
	 * 
	 * @param jobManagerAddress
	 *        the address of the job manager
	 * @return the address with which the task manager shall announce itself to the job manager
	 * @throws DiscoveryException
	 *         thrown if an error occurs during the packet exchange
	 */
	public static InetAddress getTaskManagerAddress(final InetAddress jobManagerAddress) throws DiscoveryException {

		final int magicNumber = GlobalConfiguration.getInteger(MAGICNUMBER_KEY, DEFAULT_MAGICNUMBER);
		final int discoveryPort = GlobalConfiguration.getInteger(DISCOVERYPORT_KEY, DEFAULT_DISCOVERYPORT);

		InetAddress taskManagerAddress = null;
		DatagramSocket socket = null;

		try {

			socket = new DatagramSocket();
			LOG.debug("Setting socket timeout to " + CLIENTSOCKETTIMEOUT);
			socket.setSoTimeout(CLIENTSOCKETTIMEOUT);

			final DatagramPacket responsePacket = new DatagramPacket(new byte[RESPONSE_PACKET_SIZE],
				RESPONSE_PACKET_SIZE);

			for (int retries = 0; retries < DISCOVERFAILURERETRIES; retries++) {

				final DatagramPacket addressRequest = createTaskManagerAddressRequestPacket(magicNumber);
				addressRequest.setAddress(jobManagerAddress);
				addressRequest.setPort(discoveryPort);

				LOG.debug("Sending Task Manager address request to " + addressRequest.getSocketAddress());
				socket.send(addressRequest);

				try {
					socket.receive(responsePacket);
				} catch (SocketTimeoutException ste) {
					LOG.warn("Timeout wainting for task manager address reply. Retrying...");
					continue;
				}

				if (!isPacketForUs(responsePacket, magicNumber)) {
					LOG.warn("Received packet which is not destined to this Nephele setup");
					continue;
				}

				final int packetTypeID = getPacketTypeID(responsePacket);
				if (packetTypeID != TM_ADDRESS_REPLY_ID) {
					LOG.warn("Received response of unknown type " + packetTypeID + ", discarding...");
					continue;
				}

				taskManagerAddress = extractInetAddress(responsePacket);
				break;
			}

		} catch (IOException ioe) {
			throw new DiscoveryException(StringUtils.stringifyException(ioe));
		} finally {
			if (socket != null) {
				socket.close();
			}
		}

		if (taskManagerAddress == null) {
			throw new DiscoveryException("Unable to obtain task manager address");
		}

		return taskManagerAddress;
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

		final int magicNumber = GlobalConfiguration.getInteger(MAGICNUMBER_KEY, DEFAULT_MAGICNUMBER);
		final int discoveryPort = GlobalConfiguration.getInteger(DISCOVERYPORT_KEY, DEFAULT_DISCOVERYPORT);

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

			final DatagramPacket responsePacket = new DatagramPacket(new byte[RESPONSE_PACKET_SIZE],
				RESPONSE_PACKET_SIZE);

			for (int retries = 0; retries < DISCOVERFAILURERETRIES; retries++) {

				final DatagramPacket lookupRequest = createJobManagerLookupRequestPacket(magicNumber);

				for (InetAddress broadcast : targetAddresses) {
					lookupRequest.setAddress(broadcast);
					lookupRequest.setPort(discoveryPort);
					LOG.debug("Sending discovery request to " + lookupRequest.getSocketAddress());
					socket.send(lookupRequest);
				}

				try {
					socket.receive(responsePacket);
				} catch (SocketTimeoutException ste) {
					LOG.debug("Timeout wainting for discovery reply. Retrying...");
					continue;
				}

				if (!isPacketForUs(responsePacket, magicNumber)) {
					LOG.debug("Received packet which is not destined to this Nephele setup");
					continue;
				}

				final int packetTypeID = getPacketTypeID(responsePacket);
				if (packetTypeID != JM_LOOKUP_REPLY_ID) {
					LOG.debug("Received unexpected packet type " + packetTypeID + ", discarding... ");
					continue;
				}

				final int ipcPort = extractIpcPort(responsePacket);

				// Replace port from discovery service with the actual RPC port
				// of the job manager
				if (USE_IPV6) {
					// TODO: No connection possible unless we remove the scope identifier
					if (responsePacket.getAddress() instanceof Inet6Address) {
						try {
							jobManagerAddress = new InetSocketAddress(InetAddress.getByAddress(responsePacket
								.getAddress()
								.getAddress()), ipcPort);
						} catch (UnknownHostException e) {
							throw new DiscoveryException(StringUtils.stringifyException(e));
						}
					} else {
						throw new DiscoveryException(responsePacket.getAddress() + " is not a valid IPv6 address");
					}
				} else {
					jobManagerAddress = new InetSocketAddress(responsePacket.getAddress(), ipcPort);
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

	/**
	 * Extracts an IPC port from the given datagram packet. The datagram packet must be of the type
	 * <code>JM_LOOKUP_REPLY_PACKET_ID</code>.
	 * 
	 * @param packet
	 *        the packet to extract the IPC port from.
	 * @return the extracted IPC port or <code>-1</code> if the port could not be extracted
	 */
	private static int extractIpcPort(DatagramPacket packet) {

		final byte[] data = packet.getData();

		if (data == null) {
			return -1;
		}

		if (packet.getLength() < (PAYLOAD_OFFSET + 4)) {
			return -1;
		}

		return byteArrayToInteger(data, PAYLOAD_OFFSET);
	}

	/**
	 * Extracts an {@link InetAddress} object from the given datagram packet. The datagram packet must be of the type
	 * <code>TM_ADDRESS_REPLY_PACKET_ID</code>.
	 * 
	 * @param packet
	 *        the packet to extract the address from
	 * @return the extracted address or <code>null</code> if it could not be extracted
	 */
	private static InetAddress extractInetAddress(DatagramPacket packet) {

		final byte[] data = packet.getData();

		if (data == null) {
			return null;
		}

		if (packet.getLength() < PAYLOAD_OFFSET + 8) {
			return null;
		}

		final int len = byteArrayToInteger(data, PAYLOAD_OFFSET);

		final byte[] addr = new byte[len];
		System.arraycopy(data, PAYLOAD_OFFSET + 4, addr, 0, len);

		InetAddress inetAddress = null;

		try {
			inetAddress = InetAddress.getByAddress(addr);
		} catch (UnknownHostException e) {
			return null;
		}

		return inetAddress;
	}

	/**
	 * Returns the set of broadcast addresses available to the network interfaces of this host. In case of IPv6 the set
	 * contains the IPv6 multicast address to reach all nodes on the local link. Moreover, all addresses of the loopback
	 * interfaces are added to the set.
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

		while (ie.hasMoreElements()) {
			NetworkInterface nic = ie.nextElement();
			try {
				if (!nic.isUp()) {
					continue;
				}

				if (nic.isLoopback()) {
					for (InterfaceAddress adr : nic.getInterfaceAddresses()) {
						broadcastAddresses.add(adr.getAddress());
					}
				} else {

					// check all IPs bound to network interfaces
					for (InterfaceAddress adr : nic.getInterfaceAddresses()) {

						if (adr == null) {
							continue;
						}

						// collect all broadcast addresses
						if (USE_IPV6) {
							try {
								final InetAddress interfaceAddress = adr.getAddress();
								if (interfaceAddress instanceof Inet6Address) {
									final Inet6Address ipv6Address = (Inet6Address) interfaceAddress;
									final InetAddress multicastAddress = InetAddress.getByName(IPV6MULTICASTADDRESS
										+ "%"
										+ Integer.toString(ipv6Address.getScopeId()));
									broadcastAddresses.add(multicastAddress);
								}

							} catch (UnknownHostException e) {
								LOG.error(e);
							}
						} else {
							final InetAddress broadcast = adr.getBroadcast();
							if (broadcast != null) {
								broadcastAddresses.add(broadcast);
							}
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
	 * Server side implementation of Discovery Service.
	 */
	@Override
	public void run() {

		final DatagramPacket requestPacket = new DatagramPacket(new byte[64], 64);

		final Map<Integer, Long> packetIDMap = new HashMap<Integer, Long>();

		while (this.isRunning) {

			try {
				this.serverSocket.receive(requestPacket);

				if (!isPacketForUs(requestPacket, this.magicNumber)) {
					LOG.debug("Received request packet which is not destined to this Nephele setup");
					continue;
				}

				final Integer packetID = Integer.valueOf(extractPacketID(requestPacket));
				if (packetIDMap.containsKey(packetID)) {
					LOG.debug("Request with ID " + packetID.intValue() + " already answered, discarding...");
					continue;
				} else {

					final long currentTime = System.currentTimeMillis();

					// Remove old entries
					final Iterator<Map.Entry<Integer, Long>> it = packetIDMap.entrySet().iterator();
					while (it.hasNext()) {

						final Map.Entry<Integer, Long> entry = it.next();
						if ((entry.getValue().longValue() + 5000L) < currentTime) {
							it.remove();
						}
					}

					packetIDMap.put(packetID, Long.valueOf(currentTime));
				}

				final int packetTypeID = getPacketTypeID(requestPacket);
				if (packetTypeID == JM_LOOKUP_REQUEST_ID) {

					LOG.debug("Received job manager lookup request from " + requestPacket.getSocketAddress());
					final DatagramPacket responsePacket = createJobManagerLookupReplyPacket(this.ipcPort,
						this.magicNumber);
					responsePacket.setAddress(requestPacket.getAddress());
					responsePacket.setPort(requestPacket.getPort());

					this.serverSocket.send(responsePacket);

				} else if (packetTypeID == TM_ADDRESS_REQUEST_ID) {
					LOG.debug("Received task manager address request from " + requestPacket.getSocketAddress());
					final DatagramPacket responsePacket = createTaskManagerAddressReplyPacket(requestPacket
						.getAddress(), this.magicNumber);
					responsePacket.setAddress(requestPacket.getAddress());
					responsePacket.setPort(requestPacket.getPort());

					this.serverSocket.send(responsePacket);

				} else {
					LOG.debug("Received packet of unknown type " + packetTypeID + ", discarding...");
				}

			} catch (SocketTimeoutException ste) {
				LOG.debug("Discovery service: socket timeout");
			} catch (IOException ioe) {
				if (this.isRunning) { // Ignore exception when service has been stopped
					LOG.error("Discovery service stopped working with IOException:\n" + ioe.toString());
				}
				break;
			}
		}

		// Close the socket finally
		this.serverSocket.close();
	}

	/**
	 * Serializes and writes the given integer number to the provided byte array.
	 * 
	 * @param integerToSerialize
	 *        the integer number of serialize
	 * @param offset
	 *        the offset at which to start writing inside the byte array
	 * @param byteArray
	 *        the byte array to write to
	 */
	private static void integerToByteArray(final int integerToSerialize, final int offset, final byte[] byteArray) {

		for (int i = 0; i < 4; ++i) {
			final int shift = i << 3; // i * 8
			byteArray[(offset + 3) - i] = (byte) ((integerToSerialize & (0xff << shift)) >>> shift);
		}
	}

	/**
	 * Reads and deserializes an integer number from the given byte array.
	 * 
	 * @param byteArray
	 *        the byte array to read from
	 * @param offset
	 *        the offset at which to start reading the byte array
	 * @return the deserialized integer number
	 */
	private static int byteArrayToInteger(final byte[] byteArray, final int offset) {

		int integer = 0;

		for (int i = 0; i < 4; ++i) {
			integer |= (byteArray[(offset + 3) - i] & 0xff) << (i << 3);
		}

		return integer;
	}

	/**
	 * Extracts the datagram packet's magic number and checks it matches with the local magic number.
	 * 
	 * @param packet
	 *        the packet to check
	 * @param magicNumber
	 *        the magic number identifying the discovery service
	 * @return <code>true</code> if the packet carries the magic number expected by the local service, otherwise
	 *         <code>false</code>
	 */
	private static boolean isPacketForUs(final DatagramPacket packet, final int magicNumber) {

		final byte[] data = packet.getData();

		if (data == null) {
			return false;
		}

		if (packet.getLength() < (MAGIC_NUMBER_OFFSET + 4)) {
			return false;
		}

		if (byteArrayToInteger(data, MAGIC_NUMBER_OFFSET) != magicNumber) {
			return false;
		}

		return true;
	}

	/**
	 * Extracts the packet type ID from the given datagram packet.
	 * 
	 * @param packet
	 *        the packet to extract the type ID from
	 * @return the extracted packet type ID or <code>-1</code> if the ID could not be extracted
	 */
	private static int getPacketTypeID(final DatagramPacket packet) {

		final byte[] data = packet.getData();

		if (data == null) {
			return -1;
		}

		if (packet.getLength() < (PACKET_TYPE_ID_OFFSET + 4)) {
			return -1;
		}

		return byteArrayToInteger(data, PACKET_TYPE_ID_OFFSET);
	}

	/**
	 * Generates a random packet ID.
	 * 
	 * @return a random packet ID
	 */
	private static int generateRandomPacketID() {

		return (int) (Math.random() * (double) Integer.MAX_VALUE);
	}

	/**
	 * Extracts the packet ID from the given packet.
	 * 
	 * @param packet
	 *        the packet to extract the ID from
	 * @return the extracted ID or <code>-1</code> if the ID could not be extracted
	 */
	private static int extractPacketID(final DatagramPacket packet) {

		final byte[] data = packet.getData();

		if (data == null) {
			return -1;
		}

		if (data.length < (PACKET_ID_OFFSET + 4)) {
			return -1;
		}

		return byteArrayToInteger(data, PACKET_ID_OFFSET);
	}
}
