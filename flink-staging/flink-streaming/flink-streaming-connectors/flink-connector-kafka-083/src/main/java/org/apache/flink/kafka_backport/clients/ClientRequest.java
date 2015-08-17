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

import org.apache.flink.kafka_backport.common.requests.RequestSend;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
public final class ClientRequest {

    private final long createdMs;
    private final boolean expectResponse;
    private final RequestSend request;
    private final RequestCompletionHandler callback;

    /**
     * @param createdMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param request The request
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(long createdMs, boolean expectResponse, RequestSend request, RequestCompletionHandler callback) {
        this.createdMs = createdMs;
        this.callback = callback;
        this.request = request;
        this.expectResponse = expectResponse;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse + ", callback=" + callback + ", request=" + request
                + ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestSend request() {
        return request;
    }

    public boolean hasCallback() {
        return callback != null;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTime() {
        return createdMs;
    }

}