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

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

/** Response body for listing quarantined nodes. */
public class NodeQuarantineListResponseBody implements ResponseBody {

    public static final String FIELD_NAME_QUARANTINED_NODES = "quarantinedNodes";

    @JsonProperty(FIELD_NAME_QUARANTINED_NODES)
    private final Collection<NodeQuarantineInfo> quarantinedNodes;

    @JsonCreator
    public NodeQuarantineListResponseBody(
            @JsonProperty(FIELD_NAME_QUARANTINED_NODES)
                    Collection<NodeQuarantineInfo> quarantinedNodes) {
        this.quarantinedNodes = quarantinedNodes;
    }

    public Collection<NodeQuarantineInfo> getQuarantinedNodes() {
        return quarantinedNodes;
    }

    /** Information about a quarantined node. */
    public static class NodeQuarantineInfo {

        public static final String FIELD_NAME_NODE_ID = "nodeId";
        public static final String FIELD_NAME_HOSTNAME = "hostname";
        public static final String FIELD_NAME_REASON = "reason";
        public static final String FIELD_NAME_QUARANTINE_TIMESTAMP = "quarantineTimestamp";
        public static final String FIELD_NAME_EXPIRATION_TIMESTAMP = "expirationTimestamp";

        @JsonProperty(FIELD_NAME_NODE_ID)
        private final String nodeId;

        @JsonProperty(FIELD_NAME_HOSTNAME)
        private final String hostname;

        @JsonProperty(FIELD_NAME_REASON)
        private final String reason;

        @JsonProperty(FIELD_NAME_QUARANTINE_TIMESTAMP)
        private final long quarantineTimestamp;

        @JsonProperty(FIELD_NAME_EXPIRATION_TIMESTAMP)
        private final long expirationTimestamp;

        @JsonCreator
        public NodeQuarantineInfo(
                @JsonProperty(FIELD_NAME_NODE_ID) String nodeId,
                @JsonProperty(FIELD_NAME_HOSTNAME) String hostname,
                @JsonProperty(FIELD_NAME_REASON) String reason,
                @JsonProperty(FIELD_NAME_QUARANTINE_TIMESTAMP) long quarantineTimestamp,
                @JsonProperty(FIELD_NAME_EXPIRATION_TIMESTAMP) long expirationTimestamp) {
            this.nodeId = nodeId;
            this.hostname = hostname;
            this.reason = reason;
            this.quarantineTimestamp = quarantineTimestamp;
            this.expirationTimestamp = expirationTimestamp;
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getHostname() {
            return hostname;
        }

        public String getReason() {
            return reason;
        }

        public long getQuarantineTimestamp() {
            return quarantineTimestamp;
        }

        public long getExpirationTimestamp() {
            return expirationTimestamp;
        }
    }
}
