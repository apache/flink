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
package org.apache.flink.kafka_backport.clients;

import java.util.HashMap;
import java.util.Map;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * The state of our connection to each node in the cluster.
 * 
 */
final class ClusterConnectionStates {
    private final long reconnectBackoffMs;
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<String, NodeConnectionState>();
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id The connection id to check
     * @param now The current time in MS
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return true;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet
     * @param id The connection to check
     * @param now The current time in ms
     */
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return false;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * @param id The connection to check
     * @param now The current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0;
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state == ConnectionState.DISCONNECTED) {
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Enter the connecting state for the given connection.
     * @param id The id of the connection
     * @param now The current time.
     */
    public void connecting(String id, long now) {
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * Return true iff a specific connection is connected
     * @param id The id of the connection to check
     */
    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTED;
    }

    /**
     * Return true iff we are in the process of connecting
     * @param id The id of the connection
     */
    public boolean isConnecting(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connected state for the given connection
     * @param id The connection identifier
     */
    public void connected(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CONNECTED;
    }

    /**
     * Enter the disconnected state for the given node
     * @param id The connection we have disconnected
     */
    public void disconnected(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.DISCONNECTED;
    }
    
    /**
     * Get the state of a given connection
     * @param id The id of the connection
     * @return The state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }
    
    /**
     * Get the state of a given node
     * @param id The connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }
    
    /**
     * The state of our connection to a node
     */
    private static class NodeConnectionState {

        ConnectionState state;
        long lastConnectAttemptMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttemptMs = lastConnectAttempt;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ")";
        }
    }
}