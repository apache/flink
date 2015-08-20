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
package org.apache.kafka.copied.clients;

import org.apache.kafka.copied.common.Node;
import org.apache.kafka.copied.common.protocol.ApiKeys;
import org.apache.kafka.copied.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;

/**
 * The interface for {@link NetworkClient}
 */
public interface KafkaClient extends Closeable {

    /**
     * Check if we are currently ready to send another request to the given node but don't attempt to connect if we
     * aren't.
     * 
     * @param node The node to check
     * @param now The current timestamp
     */
    public boolean isReady(Node node, long now);

    /**
     * Initiate a connection to the given node (if necessary), and return true if already connected. The readiness of a
     * node will change only when poll is invoked.
     * 
     * @param node The node to connect to.
     * @param now The current time
     * @return true iff we are ready to immediately initiate the sending of another request to the given node.
     */
    public boolean ready(Node node, long now);

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * 
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    public long connectionDelay(Node node, long now);

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    public boolean connectionFailed(Node node);

    /**
     * Queue up the given request for sending. Requests can only be sent on ready connections.
     * 
     * @param request The request
     */
    public void send(ClientRequest request);

    /**
     * Do actual reads and writes from sockets.
     * 
     * @param timeout The maximum amount of time to wait for responses in ms
     * @param now The current time in ms
     * @throws IllegalStateException If a request is sent to an unready node
     */
    public List<ClientResponse> poll(long timeout, long now);

    /**
     * Complete all in-flight requests for a given connection
     * 
     * @param id The connection to complete requests for
     * @param now The current time in ms
     * @return All requests that complete during this time period.
     */
    public List<ClientResponse> completeAll(String id, long now);

    /**
     * Complete all in-flight requests
     * 
     * @param now The current time in ms
     * @return All requests that complete during this time period.
     */
    public List<ClientResponse> completeAll(long now);

    /**
     * Choose the node with the fewest outstanding requests. This method will prefer a node with an existing connection,
     * but will potentially choose a node for which we don't yet have a connection if all existing connections are in
     * use.
     * 
     * @param now The current time in ms
     * @return The node with the fewest in-flight requests.
     */
    public Node leastLoadedNode(long now);

    /**
     * The number of currently in-flight requests for which we have not yet returned a response
     */
    public int inFlightRequestCount();

    /**
     * Get the total in-flight requests for a particular node
     * 
     * @param nodeId The id of the node
     */
    public int inFlightRequestCount(String nodeId);

    /**
     * Generate a request header for the next request
     * 
     * @param key The API key of the request
     */
    public RequestHeader nextRequestHeader(ApiKeys key);

    /**
     * Wake up the client if it is currently blocked waiting for I/O
     */
    public void wakeup();

}