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

import org.apache.kafka.copied.common.protocol.types.Struct;

/**
 * A response from the server. Contains both the body of the response as well as the correlated request that was
 * originally sent.
 */
public class ClientResponse {

    private final long received;
    private final boolean disconnected;
    private final ClientRequest request;
    private final Struct responseBody;

    /**
     * @param request The original request
     * @param received The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param responseBody The response contents (or null) if we disconnected or no response was expected
     */
    public ClientResponse(ClientRequest request, long received, boolean disconnected, Struct responseBody) {
        super();
        this.received = received;
        this.disconnected = disconnected;
        this.request = request;
        this.responseBody = responseBody;
    }

    public long receivedTime() {
        return received;
    }

    public boolean wasDisconnected() {
        return disconnected;
    }

    public ClientRequest request() {
        return request;
    }

    public Struct responseBody() {
        return responseBody;
    }

    public boolean hasResponse() {
        return responseBody != null;
    }

    public long requestLatencyMs() {
        return receivedTime() - this.request.createdTime();
    }

    @Override
    public String toString() {
        return "ClientResponse(received=" + received +
               ", disconnected=" +
               disconnected +
               ", request=" +
               request +
               ", responseBody=" +
               responseBody +
               ")";
    }

}
