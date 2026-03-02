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

import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Response body for retrieving the list of quarantined nodes. */
public class NodeQuarantineListManagementResponseBody implements ResponseBody {

    public static final String FIELD_NAME_QUARANTINED_NODES = "quarantinedNodes";

    @JsonProperty(FIELD_NAME_QUARANTINED_NODES)
    private final List<QuarantinedNodeInfo> quarantinedNodes;

    @JsonCreator
    public NodeQuarantineListManagementResponseBody(
            @JsonProperty(FIELD_NAME_QUARANTINED_NODES) Collection<BlockedNode> quarantinedNodes) {
        this.quarantinedNodes =
                Preconditions.checkNotNull(quarantinedNodes).stream()
                        .map(QuarantinedNodeInfo::fromBlockedNode)
                        .collect(Collectors.toList());
    }

    public List<QuarantinedNodeInfo> getQuarantinedNodes() {
        return quarantinedNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeQuarantineListManagementResponseBody that =
                (NodeQuarantineListManagementResponseBody) o;
        return Objects.equals(quarantinedNodes, that.quarantinedNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quarantinedNodes);
    }

    @Override
    public String toString() {
        return "NodeQuarantineListManagementResponseBody{"
                + "quarantinedNodes="
                + quarantinedNodes
                + '}';
    }

    /** Information about a quarantined node for JSON serialization. */
    public static class QuarantinedNodeInfo {
        public static final String FIELD_NAME_NODE_ID = "nodeId";
        public static final String FIELD_NAME_CAUSE = "cause";
        public static final String FIELD_NAME_END_TIMESTAMP = "endTimestamp";

        @JsonProperty(FIELD_NAME_NODE_ID)
        private final String nodeId;

        @JsonProperty(FIELD_NAME_CAUSE)
        private final String cause;

        @JsonProperty(FIELD_NAME_END_TIMESTAMP)
        private final long endTimestamp;

        @JsonCreator
        public QuarantinedNodeInfo(
                @JsonProperty(FIELD_NAME_NODE_ID) String nodeId,
                @JsonProperty(FIELD_NAME_CAUSE) String cause,
                @JsonProperty(FIELD_NAME_END_TIMESTAMP) long endTimestamp) {
            this.nodeId = Preconditions.checkNotNull(nodeId);
            this.cause = Preconditions.checkNotNull(cause);
            this.endTimestamp = endTimestamp;
        }

        public static QuarantinedNodeInfo fromBlockedNode(BlockedNode blockedNode) {
            return new QuarantinedNodeInfo(
                    blockedNode.getNodeId(), blockedNode.getCause(), blockedNode.getEndTimestamp());
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getCause() {
            return cause;
        }

        public long getEndTimestamp() {
            return endTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QuarantinedNodeInfo that = (QuarantinedNodeInfo) o;
            return endTimestamp == that.endTimestamp
                    && Objects.equals(nodeId, that.nodeId)
                    && Objects.equals(cause, that.cause);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, cause, endTimestamp);
        }

        @Override
        public String toString() {
            return "QuarantinedNodeInfo{"
                    + "nodeId='"
                    + nodeId
                    + '\''
                    + ", cause='"
                    + cause
                    + '\''
                    + ", endTimestamp="
                    + endTimestamp
                    + '}';
        }
    }
}
