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

/** Response body for retrieving the list of blocked nodes. */
public class BlocklistListResponseBody implements ResponseBody {

    public static final String FIELD_NAME_BLOCKED_NODES = "blockedNodes";

    @JsonProperty(FIELD_NAME_BLOCKED_NODES)
    private final List<BlockedNodeInfo> blockedNodes;

    @JsonCreator
    public BlocklistListResponseBody(
            @JsonProperty(FIELD_NAME_BLOCKED_NODES) Collection<BlockedNode> blockedNodes) {
        this.blockedNodes =
                Preconditions.checkNotNull(blockedNodes).stream()
                        .map(BlockedNodeInfo::fromBlockedNode)
                        .collect(Collectors.toList());
    }

    public List<BlockedNodeInfo> getBlockedNodes() {
        return blockedNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlocklistListResponseBody that = (BlocklistListResponseBody) o;
        return Objects.equals(blockedNodes, that.blockedNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blockedNodes);
    }

    @Override
    public String toString() {
        return "BlocklistListResponseBody{" + "blockedNodes=" + blockedNodes + '}';
    }

    /** Information about a blocked node for JSON serialization. */
    public static class BlockedNodeInfo {
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
        public BlockedNodeInfo(
                @JsonProperty(FIELD_NAME_NODE_ID) String nodeId,
                @JsonProperty(FIELD_NAME_CAUSE) String cause,
                @JsonProperty(FIELD_NAME_END_TIMESTAMP) long endTimestamp) {
            this.nodeId = Preconditions.checkNotNull(nodeId);
            this.cause = Preconditions.checkNotNull(cause);
            this.endTimestamp = endTimestamp;
        }

        public static BlockedNodeInfo fromBlockedNode(BlockedNode blockedNode) {
            return new BlockedNodeInfo(
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
            BlockedNodeInfo that = (BlockedNodeInfo) o;
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
            return "BlockedNodeInfo{"
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
