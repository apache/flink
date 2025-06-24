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

package org.apache.flink.runtime.blocklist;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class represents a blocked node record. */
public class BlockedNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final String cause;

    private final long endTimestamp;

    public BlockedNode(String nodeId, String cause, long endTimestamp) {
        this.nodeId = checkNotNull(nodeId);
        this.cause = checkNotNull(cause);
        this.endTimestamp = endTimestamp;
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
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof BlockedNode) {
            BlockedNode other = (BlockedNode) obj;
            return nodeId.equals(other.nodeId)
                    && cause.equals(other.cause)
                    && endTimestamp == other.endTimestamp;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "BlockedNode{"
                + "id:"
                + nodeId
                + ",cause:"
                + cause
                + ",endTimestamp:"
                + endTimestamp
                + "}";
    }
}
