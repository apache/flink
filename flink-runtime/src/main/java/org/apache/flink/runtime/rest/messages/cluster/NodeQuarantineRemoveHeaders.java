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

package org.apache.flink.runtime.rest.messages.cluster;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for removing a node from the node quarantine. */
public class NodeQuarantineRemoveHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody,
                NodeQuarantineRemoveResponseBody,
                NodeQuarantineRemoveMessageParameters> {

    private static final NodeQuarantineRemoveHeaders INSTANCE = new NodeQuarantineRemoveHeaders();

    public static final String URL = "/cluster/node-quarantine/:nodeId";

    private NodeQuarantineRemoveHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<NodeQuarantineRemoveResponseBody> getResponseClass() {
        return NodeQuarantineRemoveResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public NodeQuarantineRemoveMessageParameters getUnresolvedMessageParameters() {
        return new NodeQuarantineRemoveMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.DELETE;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static NodeQuarantineRemoveHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Removes a node from the quarantine list to allow new slots to be allocated on it.";
    }
}
