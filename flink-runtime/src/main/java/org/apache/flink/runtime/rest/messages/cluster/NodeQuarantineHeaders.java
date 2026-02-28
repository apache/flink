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
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * Headers for node quarantine REST API endpoints.
 */
public class NodeQuarantineHeaders {

    /**
     * Headers for quarantining a node (POST).
     */
    public static class QuarantineNodeHeaders implements MessageHeaders<NodeQuarantineRequestBody, NodeQuarantineResponseBody, NodeIdMessageParameters> {

        public static final QuarantineNodeHeaders INSTANCE = new QuarantineNodeHeaders();

        private static final String URL = "/cluster/nodes/:nodeid/quarantine";

        private QuarantineNodeHeaders() {}

        @Override
        public Class<NodeQuarantineRequestBody> getRequestClass() {
            return NodeQuarantineRequestBody.class;
        }

        @Override
        public Class<NodeQuarantineResponseBody> getResponseClass() {
            return NodeQuarantineResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "Quarantine a node to prevent new slots from being allocated on it.";
        }

        @Override
        public NodeIdMessageParameters getUnresolvedMessageParameters() {
            return new NodeIdMessageParameters();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return URL;
        }

        @Override
        public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V1);
        }
    }

    /**
     * Headers for removing node quarantine (DELETE).
     */
    public static class RemoveQuarantineHeaders implements MessageHeaders<EmptyRequestBody, NodeQuarantineResponseBody, NodeIdMessageParameters> {

        public static final RemoveQuarantineHeaders INSTANCE = new RemoveQuarantineHeaders();

        private static final String URL = "/cluster/nodes/:nodeid/quarantine";

        private RemoveQuarantineHeaders() {}

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public Class<NodeQuarantineResponseBody> getResponseClass() {
            return NodeQuarantineResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "Remove quarantine from a node to allow new slots to be allocated on it.";
        }

        @Override
        public NodeIdMessageParameters getUnresolvedMessageParameters() {
            return new NodeIdMessageParameters();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.DELETE;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return URL;
        }

        @Override
        public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V1);
        }
    }

    /**
     * Headers for listing quarantined nodes (GET).
     */
    public static class ListQuarantinedNodesHeaders implements MessageHeaders<EmptyRequestBody, NodeQuarantineListResponseBody, MessageParameters> {

        public static final ListQuarantinedNodesHeaders INSTANCE = new ListQuarantinedNodesHeaders();

        private static final String URL = "/cluster/nodes/quarantine";

        private ListQuarantinedNodesHeaders() {}

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public Class<NodeQuarantineListResponseBody> getResponseClass() {
            return NodeQuarantineListResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "List all quarantined nodes.";
        }

        @Override
        public MessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return URL;
        }

        @Override
        public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
            return Collections.singleton(RuntimeRestAPIVersion.V1);
        }
    }

    /**
     * Path parameter for node ID.
     */
    public static class NodeIdPathParameter extends MessagePathParameter<String> {

        public static final String KEY = "nodeid";

        public NodeIdPathParameter() {
            super(KEY);
        }

        @Override
        protected String convertFromString(String value) {
            return value;
        }

        @Override
        protected String convertToString(String value) {
            return value;
        }

        @Override
        public String getDescription() {
            return "The ID of the node.";
        }
    }

    /**
     * Message parameters containing NodeIdPathParameter.
     */
    public static class NodeIdMessageParameters extends MessageParameters {

        private final NodeIdPathParameter nodeIdPathParameter = new NodeIdPathParameter();

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.singleton(nodeIdPathParameter);
        }

        @Override
        public Collection<MessageQueryParameter<?>> getQueryParameters() {
            return Collections.emptyList();
        }
    }

    /**
     * Empty message parameters for endpoints without parameters.
     */
    public static class EmptyMessageParameters extends MessageParameters {

        private static final EmptyMessageParameters INSTANCE = new EmptyMessageParameters();

        private EmptyMessageParameters() {}

        public static EmptyMessageParameters getInstance() {
            return INSTANCE;
        }

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.emptyList();
        }

        @Override
        public Collection<MessageQueryParameter<?>> getQueryParameters() {
            return Collections.emptyList();
        }
    }
}