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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketOptions;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/** Utility for various network related tasks (such as finding free ports). */
@Internal
public class NetUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

    /** The wildcard address to listen on all interfaces (either 0.0.0.0 or ::). */
    private static final String WILDCARD_ADDRESS =
            new InetSocketAddress(0).getAddress().getHostAddress();

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
        if (dotPos == -1) {
            return fqdn;
        } else {
            return fqdn.substring(0, dotPos);
        }
    }

    /**
     * Converts a string of the form "host:port" into an {@link URL}.
     *
     * @param hostPort The "host:port" string.
     * @return The converted URL.
     */
    public static URL getCorrectHostnamePort(String hostPort) {
        return validateHostPortString(hostPort);
    }

    /**
     * Converts a string of the form "host:port" into an {@link InetSocketAddress}.
     *
     * @param hostPort The "host:port" string.
     * @return The converted InetSocketAddress.
     */
    public static InetSocketAddress parseHostPortAddress(String hostPort) {
        URL url = validateHostPortString(hostPort);
        return new InetSocketAddress(url.getHost(), url.getPort());
    }

    /**
     * Validates if the given String represents a hostname:port.
     *
     * <p>Works also for ipv6.
     *
     * <p>See:
     * http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
     *
     * @return URL object for accessing host and port
     */
    private static URL validateHostPortString(String hostPort) {
        try {
            URL u = new URL("http://" + hostPort);
            if (u.getHost() == null) {
                throw new IllegalArgumentException(
                        "The given host:port ('" + hostPort + "') doesn't contain a valid host");
            }
            if (u.getPort() == -1) {
                throw new IllegalArgumentException(
                        "The given host:port ('" + hostPort + "') doesn't contain a valid port");
            }
            return u;
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "The given host:port ('" + hostPort + "') is invalid", e);
        }
    }

    /**
     * Calls {@link ServerSocket#accept()} on the provided server socket, suppressing any thrown
     * {@link SocketTimeoutException}s. This is a workaround for the underlying JDK-8237858 bug in
     * JDK 11 that can cause errant SocketTimeoutExceptions to be thrown at unexpected times.
     *
     * <p>This method expects the provided ServerSocket has no timeout set (SO_TIMEOUT of 0),
     * indicating an infinite timeout. It will suppress all SocketTimeoutExceptions, even if a
     * ServerSocket with a non-zero timeout is passed in.
     *
     * @param serverSocket a ServerSocket with {@link SocketOptions#SO_TIMEOUT SO_TIMEOUT} set to 0;
     *     if SO_TIMEOUT is greater than 0, then this method will suppress SocketTimeoutException;
     *     must not be null; SO_TIMEOUT option must be set to 0
     * @return the new Socket
     * @exception IOException see {@link ServerSocket#accept()}
     * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8237858">JDK-8237858</a>
     */
    public static Socket acceptWithoutTimeout(ServerSocket serverSocket) throws IOException {
        Preconditions.checkArgument(
                serverSocket.getSoTimeout() == 0, "serverSocket SO_TIMEOUT option must be 0");
        while (true) {
            try {
                return serverSocket.accept();
            } catch (SocketTimeoutException exception) {
                // This should be impossible given that the socket timeout is set to zero
                // which indicates an infinite timeout. This is due to the underlying JDK-8237858
                // bug. We retry the accept call indefinitely to replicate the expected behavior.
            }
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
            } catch (IOException ignored) {
            }
        }

        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    // ------------------------------------------------------------------------
    //  Encoding of IP addresses for URLs
    // ------------------------------------------------------------------------

    /**
     * Returns an address in a normalized format for Akka. When an IPv6 address is specified, it
     * normalizes the IPv6 address to avoid complications with the exact URL match policy of Akka.
     *
     * @param host The hostname, IPv4 or IPv6 address
     * @return host which will be normalized if it is an IPv6 address
     */
    public static String unresolvedHostToNormalizedString(String host) {
        // Return loopback interface address if host is null
        // This represents the behavior of {@code InetAddress.getByName } and RFC 3330
        if (host == null) {
            host = InetAddress.getLoopbackAddress().getHostAddress();
        } else {
            host = host.trim().toLowerCase();
            if (host.startsWith("[") && host.endsWith("]")) {
                String address = host.substring(1, host.length() - 1);
                if (IPAddressUtil.isIPv6LiteralAddress(address)) {
                    host = address;
                }
            }
        }

        // normalize and valid address
        if (IPAddressUtil.isIPv6LiteralAddress(host)) {
            byte[] ipV6Address = IPAddressUtil.textToNumericFormatV6(host);
            host = getIPv6UrlRepresentation(ipV6Address);
        } else if (!IPAddressUtil.isIPv4LiteralAddress(host)) {
            try {
                // We don't allow these in hostnames
                Preconditions.checkArgument(!host.startsWith("."));
                Preconditions.checkArgument(!host.endsWith("."));
                Preconditions.checkArgument(!host.contains(":"));
            } catch (Exception e) {
                throw new IllegalConfigurationException("The configured hostname is not valid", e);
            }
        }

        return host;
    }

    /**
     * Returns a valid address for Akka. It returns a String of format 'host:port'. When an IPv6
     * address is specified, it normalizes the IPv6 address to avoid complications with the exact
     * URL match policy of Akka.
     *
     * @param host The hostname, IPv4 or IPv6 address
     * @param port The port
     * @return host:port where host will be normalized if it is an IPv6 address
     */
    public static String unresolvedHostAndPortToNormalizedString(String host, int port) {
        Preconditions.checkArgument(isValidHostPort(port), "Port is not within the valid range,");
        return unresolvedHostToNormalizedString(host) + ":" + port;
    }

    /**
     * Encodes an IP address properly as a URL string. This method makes sure that IPv6 addresses
     * have the proper formatting to be included in URLs.
     *
     * @param address The IP address to encode.
     * @return The proper URL string encoded IP address.
     */
    public static String ipAddressToUrlString(InetAddress address) {
        if (address == null) {
            throw new NullPointerException("address is null");
        } else if (address instanceof Inet4Address) {
            return address.getHostAddress();
        } else if (address instanceof Inet6Address) {
            return getIPv6UrlRepresentation((Inet6Address) address);
        } else {
            throw new IllegalArgumentException("Unrecognized type of InetAddress: " + address);
        }
    }

    /**
     * Encodes an IP address and port to be included in URL. in particular, this method makes sure
     * that IPv6 addresses have the proper formatting to be included in URLs.
     *
     * @param address The address to be included in the URL.
     * @param port The port for the URL address.
     * @return The proper URL string encoded IP address and port.
     */
    public static String ipAddressAndPortToUrlString(InetAddress address, int port) {
        return ipAddressToUrlString(address) + ':' + port;
    }

    /**
     * Encodes an IP address and port to be included in URL. in particular, this method makes sure
     * that IPv6 addresses have the proper formatting to be included in URLs.
     *
     * @param address The socket address with the IP address and port.
     * @return The proper URL string encoded IP address and port.
     */
    public static String socketAddressToUrlString(InetSocketAddress address) {
        if (address.isUnresolved()) {
            throw new IllegalArgumentException(
                    "Address cannot be resolved: " + address.getHostString());
        }
        return ipAddressAndPortToUrlString(address.getAddress(), address.getPort());
    }

    /**
     * Normalizes and encodes a hostname and port to be included in URL. In particular, this method
     * makes sure that IPv6 address literals have the proper formatting to be included in URLs.
     *
     * @param host The address to be included in the URL.
     * @param port The port for the URL address.
     * @return The proper URL string encoded IP address and port.
     * @throws java.net.UnknownHostException Thrown, if the hostname cannot be translated into a
     *     URL.
     */
    public static String hostAndPortToUrlString(String host, int port) throws UnknownHostException {
        return ipAddressAndPortToUrlString(InetAddress.getByName(host), port);
    }

    /**
     * Creates a compressed URL style representation of an Inet6Address.
     *
     * <p>This method copies and adopts code from Google's Guava library. We re-implement this here
     * in order to reduce dependency on Guava. The Guava library has frequently caused dependency
     * conflicts in the past.
     */
    private static String getIPv6UrlRepresentation(Inet6Address address) {
        return getIPv6UrlRepresentation(address.getAddress());
    }

    /**
     * Creates a compressed URL style representation of an Inet6Address.
     *
     * <p>This method copies and adopts code from Google's Guava library. We re-implement this here
     * in order to reduce dependency on Guava. The Guava library has frequently caused dependency
     * conflicts in the past.
     */
    private static String getIPv6UrlRepresentation(byte[] addressBytes) {
        // first, convert bytes to 16 bit chunks
        int[] hextets = new int[8];
        for (int i = 0; i < hextets.length; i++) {
            hextets[i] = (addressBytes[2 * i] & 0xFF) << 8 | (addressBytes[2 * i + 1] & 0xFF);
        }

        // now, find the sequence of zeros that should be compressed
        int bestRunStart = -1;
        int bestRunLength = -1;
        int runStart = -1;
        for (int i = 0; i < hextets.length + 1; i++) {
            if (i < hextets.length && hextets[i] == 0) {
                if (runStart < 0) {
                    runStart = i;
                }
            } else if (runStart >= 0) {
                int runLength = i - runStart;
                if (runLength > bestRunLength) {
                    bestRunStart = runStart;
                    bestRunLength = runLength;
                }
                runStart = -1;
            }
        }
        if (bestRunLength >= 2) {
            Arrays.fill(hextets, bestRunStart, bestRunStart + bestRunLength, -1);
        }

        // convert into text form
        StringBuilder buf = new StringBuilder(40);
        buf.append('[');

        boolean lastWasNumber = false;
        for (int i = 0; i < hextets.length; i++) {
            boolean thisIsNumber = hextets[i] >= 0;
            if (thisIsNumber) {
                if (lastWasNumber) {
                    buf.append(':');
                }
                buf.append(Integer.toHexString(hextets[i]));
            } else {
                if (i == 0 || lastWasNumber) {
                    buf.append("::");
                }
            }
            lastWasNumber = thisIsNumber;
        }
        buf.append(']');
        return buf.toString();
    }

    // ------------------------------------------------------------------------
    //  Port range parsing
    // ------------------------------------------------------------------------

    /**
     * Returns an iterator over available ports defined by the range definition.
     *
     * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.
     * @return Set of ports from the range definition
     * @throws NumberFormatException If an invalid string is passed.
     */
    public static Iterator<Integer> getPortRangeFromString(String rangeDefinition)
            throws NumberFormatException {
        final String[] ranges = rangeDefinition.trim().split(",");

        UnionIterator<Integer> iterators = new UnionIterator<>();

        for (String rawRange : ranges) {
            Iterator<Integer> rangeIterator;
            String range = rawRange.trim();
            int dashIdx = range.indexOf('-');
            if (dashIdx == -1) {
                // only one port in range:
                final int port = Integer.valueOf(range);
                if (!isValidHostPort(port)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + port
                                    + ".");
                }
                rangeIterator = Collections.singleton(Integer.valueOf(range)).iterator();
            } else {
                // evaluate range
                final int start = Integer.valueOf(range.substring(0, dashIdx));
                if (!isValidHostPort(start)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + start
                                    + ".");
                }
                final int end = Integer.valueOf(range.substring(dashIdx + 1, range.length()));
                if (!isValidHostPort(end)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + end
                                    + ".");
                }
                rangeIterator =
                        new Iterator<Integer>() {
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

        return iterators;
    }

    /**
     * Tries to allocate a socket from the given sets of ports.
     *
     * @param portsIterator A set of ports to choose from.
     * @param factory A factory for creating the SocketServer
     * @return null if no port was available or an allocated socket.
     */
    public static ServerSocket createSocketFromPorts(
            Iterator<Integer> portsIterator, SocketFactory factory) {
        while (portsIterator.hasNext()) {
            int port = portsIterator.next();
            LOG.debug("Trying to open socket on port {}", port);
            try {
                return factory.createSocket(port);
            } catch (IOException | IllegalArgumentException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unable to allocate socket on port", e);
                } else {
                    LOG.info(
                            "Unable to allocate on port {}, due to error: {}",
                            port,
                            e.getMessage());
                }
            }
        }
        return null;
    }

    /**
     * Returns the wildcard address to listen on all interfaces.
     *
     * @return Either 0.0.0.0 or :: depending on the IP setup.
     */
    public static String getWildcardIPAddress() {
        return WILDCARD_ADDRESS;
    }

    /** A factory for a local socket from port number. */
    @FunctionalInterface
    public interface SocketFactory {
        ServerSocket createSocket(int port) throws IOException;
    }

    /**
     * Check whether the given port is in right range when connecting to somewhere.
     *
     * @param port the port to check
     * @return true if the number in the range 1 to 65535
     */
    public static boolean isValidClientPort(int port) {
        return 1 <= port && port <= 65535;
    }

    /**
     * check whether the given port is in right range when getting port from local system.
     *
     * @param port the port to check
     * @return true if the number in the range 0 to 65535
     */
    public static boolean isValidHostPort(int port) {
        return 0 <= port && port <= 65535;
    }
}
