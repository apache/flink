/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common.network;

import org.apache.kafka.copied.common.KafkaException;
import org.apache.kafka.copied.common.MetricName;
import org.apache.kafka.copied.common.metrics.Measurable;
import org.apache.kafka.copied.common.metrics.MetricConfig;
import org.apache.kafka.copied.common.metrics.Metrics;
import org.apache.kafka.copied.common.metrics.Sensor;
import org.apache.kafka.copied.common.metrics.stats.Avg;
import org.apache.kafka.copied.common.metrics.stats.Count;
import org.apache.kafka.copied.common.metrics.stats.Max;
import org.apache.kafka.copied.common.metrics.stats.Rate;
import org.apache.kafka.copied.common.utils.SystemTime;
import org.apache.kafka.copied.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 * 
 * <pre>
 * nioSelector.connect(42, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 * 
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 * 
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 * 
 * <pre>
 * List&lt;NetworkRequest&gt; requestsToSend = Arrays.asList(new NetworkRequest(0, myBytes), new NetworkRequest(1, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS, requestsToSend);
 * </pre>
 * 
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 * 
 * This class is not thread safe!
 */
public class Selector implements Selectable {

    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    private final java.nio.channels.Selector nioSelector;
    private final Map<String, SelectionKey> keys;
    private final List<Send> completedSends;
    private final List<NetworkReceive> completedReceives;
    private final List<String> disconnected;
    private final List<String> connected;
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;
    private final Map<String, Long> lruConnections;
    private final long connectionsMaxIdleNanos;
    private final int maxReceiveSize;
    private final boolean metricsPerConnection;
    private long currentTimeNanos;
    private long nextIdleCloseCheckTime;


    /**
     * Create a new nioSelector
     */
    public Selector(int maxReceiveSize, long connectionMaxIdleMs, Metrics metrics, Time time, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.connectionsMaxIdleNanos = connectionMaxIdleMs * 1000 * 1000;
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;
        this.keys = new HashMap<String, SelectionKey>();
        this.completedSends = new ArrayList<Send>();
        this.completedReceives = new ArrayList<NetworkReceive>();
        this.connected = new ArrayList<String>();
        this.disconnected = new ArrayList<String>();
        this.failedSends = new ArrayList<String>();
        this.sensors = new SelectorMetrics(metrics);
        // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
        this.lruConnections = new LinkedHashMap<String, Long>(16, .75F, true);
        currentTimeNanos = new SystemTime().nanoseconds();
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
        this.metricsPerConnection = metricsPerConnection;
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, Map<String, String> metricTags) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, metricTags, true);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long, List)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.keys.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);

        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        Socket socket = channel.socket();
        socket.setKeepAlive(true);
        socket.setSendBufferSize(sendBufferSize);
        socket.setReceiveBufferSize(receiveBufferSize);
        socket.setTcpNoDelay(true);
        try {
            channel.connect(address);
        } catch (UnresolvedAddressException e) {
            channel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        SelectionKey key = channel.register(this.nioSelector, SelectionKey.OP_CONNECT);
        key.attach(new Transmissions(id));
        this.keys.put(id, key);
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     */
    public void register(String id, SocketChannel channel) throws ClosedChannelException {
        SelectionKey key = channel.register(nioSelector, SelectionKey.OP_READ);
        key.attach(new Transmissions(id));
        this.keys.put(id, key);
    }

    /**
     * Disconnect any connections for the given id (if there are any). The disconnection is asynchronous and will not be
     * processed until the next {@link #poll(long, List) poll()} call.
     */
    @Override
    public void disconnect(String id) {
        SelectionKey key = this.keys.get(id);
        if (key != null)
            key.cancel();
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new LinkedList<String>(keys.keySet());
        for (String id: connections)
            close(id);

        try {
            this.nioSelector.close();
        } catch (IOException e) {
            log.error("Exception closing nioSelector:", e);
        }
    }

    /**
     * Queue the given request for sending in the subsequent {@poll(long)} calls
     * @param send The request to send
     */
    public void send(Send send) {
        SelectionKey key = keyForId(send.destination());
        Transmissions transmissions = transmissions(key);
        if (transmissions.hasSend())
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        transmissions.send = send;
        try {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            close(transmissions.id);
            this.failedSends.add(send.destination());
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     * 
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each {@link #poll(long, List)} call and repopulated by the call if any
     * completed I/O.
     * 
     * @param timeout The amount of time to wait, in milliseconds. If negative, wait indefinitely.
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        clear();

        /* check ready keys */
        long startSelect = time.nanoseconds();
        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        currentTimeNanos = endSelect;
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (readyKeys > 0) {
            Set<SelectionKey> keys = this.nioSelector.selectedKeys();
            Iterator<SelectionKey> iter = keys.iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                Transmissions transmissions = transmissions(key);
                SocketChannel channel = channel(key);

                // register all per-connection metrics at once
                sensors.maybeRegisterConnectionMetrics(transmissions.id);
                lruConnections.put(transmissions.id, currentTimeNanos);

                try {
                    /* complete any connections that have finished their handshake */
                    if (key.isConnectable()) {
                        channel.finishConnect();
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        this.connected.add(transmissions.id);
                        this.sensors.connectionCreated.record();
                        log.debug("Connection {} created", transmissions.id);
                    }

                    /* read from any connections that have readable data */
                    if (key.isReadable()) {
                        if (!transmissions.hasReceive())
                            transmissions.receive = new NetworkReceive(maxReceiveSize, transmissions.id);
                        try {
                            transmissions.receive.readFrom(channel);
                        } catch (InvalidReceiveException e) {
                            log.error("Invalid data received from " + transmissions.id + " closing connection", e);
                            close(transmissions.id);
                            this.disconnected.add(transmissions.id);
                            throw e;
                        }
                        if (transmissions.receive.complete()) {
                            transmissions.receive.payload().rewind();
                            this.completedReceives.add(transmissions.receive);
                            this.sensors.recordBytesReceived(transmissions.id, transmissions.receive.payload().limit());
                            transmissions.clearReceive();
                        }
                    }

                    /* write to any sockets that have space in their buffer and for which we have data */
                    if (key.isWritable()) {
                        transmissions.send.writeTo(channel);
                        if (transmissions.send.completed()) {
                            this.completedSends.add(transmissions.send);
                            this.sensors.recordBytesSent(transmissions.id, transmissions.send.size());
                            transmissions.clearSend();
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }

                    /* cancel any defunct sockets */
                    if (!key.isValid()) {
                        close(transmissions.id);
                        this.disconnected.add(transmissions.id);
                    }
                } catch (IOException e) {
                    String desc = socketDescription(channel);
                    if (e instanceof EOFException || e instanceof ConnectException)
                        log.debug("Connection {} disconnected", desc);
                    else
                        log.warn("Error in I/O with connection to {}", desc, e);
                    close(transmissions.id);
                    this.disconnected.add(transmissions.id);
                }
            }
        }
        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());
        maybeCloseOldestConnection();
    }

    private String socketDescription(SocketChannel channel) {
        Socket socket = channel.socket();
        if (socket == null)
            return "[unconnected socket]";
        else if (socket.getInetAddress() != null)
            return socket.getInetAddress().toString();
        else
            return socket.getLocalAddress().toString();
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        mute(this.keyForId(id));
    }

    private void mute(SelectionKey key) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
    }

    @Override
    public void unmute(String id) {
        unmute(this.keyForId(id));
    }

    private void unmute(SelectionKey key) {
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }

    @Override
    public void muteAll() {
        for (SelectionKey key : this.keys.values())
            mute(key);
    }

    @Override
    public void unmuteAll() {
        for (SelectionKey key : this.keys.values())
            unmute(key);
    }

    private void maybeCloseOldestConnection() {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
                Long connectionLastActiveTime = oldestConnectionEntry.getValue();
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    String connectionId = oldestConnectionEntry.getKey();
                    if (log.isTraceEnabled())
                        log.trace("About to close the idle connection from " + connectionId
                                + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis");

                    disconnected.add(connectionId);
                    close(connectionId);
                }
            }
        }
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     * 
     * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
     * @return The number of keys ready
     * @throws IOException
     */
    private int select(long ms) throws IOException {
        if (ms == 0L)
            return this.nioSelector.selectNow();
        else if (ms < 0L)
            return this.nioSelector.select();
        else
            return this.nioSelector.select(ms);
    }

    /**
     * Begin closing this connection
     */
    public void close(String id) {
        SelectionKey key = keyForId(id);
        lruConnections.remove(id);
        SocketChannel channel = channel(key);
        Transmissions trans = transmissions(key);
        if (trans != null) {
            this.keys.remove(trans.id);
            trans.clearReceive();
            trans.clearSend();
        }
        key.attach(null);
        key.cancel();
        try {
            channel.socket().close();
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", trans.id, e);
        }
        this.sensors.connectionClosed.record();
    }

    /**
     * Get the selection key associated with this numeric id
     */
    private SelectionKey keyForId(String id) {
        SelectionKey key = this.keys.get(id);
        if (key == null)
            throw new IllegalStateException("Attempt to write to socket for which there is no open connection. Connection id " + id + " existing connections " + keys.keySet().toString());
        return key;
    }

    /**
     * Get the transmissions for the given connection
     */
    private Transmissions transmissions(SelectionKey key) {
        return (Transmissions) key.attachment();
    }

    /**
     * Get the socket channel associated with this selection key
     */
    private SocketChannel channel(SelectionKey key) {
        return (SocketChannel) key.channel();
    }

    /**
     * The id and in-progress send and receive associated with a connection
     */
    private static class Transmissions {
        public String id;
        public Send send;
        public NetworkReceive receive;

        public Transmissions(String id) {
            this.id = id;
        }

        public boolean hasSend() {
            return this.send != null;
        }

        public void clearSend() {
            this.send = null;
        }

        public boolean hasReceive() {
            return this.receive != null;
        }

        public void clearReceive() {
            this.receive = null;
        }
    }

    private class SelectorMetrics {
        private final Metrics metrics;
        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = this.metrics.sensor("connections-closed:" + tagsSuffix.toString());
            MetricName metricName = new MetricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = this.metrics.sensor("connections-created:" + tagsSuffix.toString());
            metricName = new MetricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = this.metrics.sensor("bytes-sent-received:" + tagsSuffix.toString());
            metricName = new MetricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = this.metrics.sensor("bytes-sent:" + tagsSuffix.toString(), bytesTransferred);
            metricName = new MetricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = new MetricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = new MetricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = new MetricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = this.metrics.sensor("bytes-received:" + tagsSuffix.toString(), bytesTransferred);
            metricName = new MetricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = new MetricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = this.metrics.sensor("select-time:" + tagsSuffix.toString());
            metricName = new MetricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = new MetricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = new MetricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = this.metrics.sensor("io-time:" + tagsSuffix.toString());
            metricName = new MetricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = new MetricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = new MetricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return keys.size();
                }
            });
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<String, String>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = this.metrics.sensor(nodeRequestName);
                    MetricName metricName = new MetricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = new MetricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = new MetricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = new MetricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = this.metrics.sensor(nodeResponseName);
                    metricName = new MetricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = new MetricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = this.metrics.sensor(nodeTimeName);
                    metricName = new MetricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = new MetricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }
    }

}
