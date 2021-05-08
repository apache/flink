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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Flame Graph representation for a job vertex.
 *
 * <p>Statistics are gathered by sampling stack traces of running tasks.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobVertexFlameGraph implements ResponseBody {

    private static final String FIELD_NAME_END_TIMESTAMP = "endTimestamp";
    private static final String FIELD_NAME_DATA = "data";

    /** End time stamp of the corresponding sample. */
    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final long endTimestamp;

    @JsonProperty(FIELD_NAME_DATA)
    private final Node root;

    @JsonCreator
    public JobVertexFlameGraph(
            @JsonProperty(FIELD_NAME_END_TIMESTAMP) long endTimestamp,
            @JsonProperty(FIELD_NAME_DATA) Node root) {
        this.endTimestamp = endTimestamp;
        this.root = root;
    }

    @JsonIgnore
    public long getEndTime() {
        return endTimestamp;
    }

    @JsonIgnore
    public Node getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "OperatorFlameGraph: endTimestamp=" + endTimestamp + "\n" + getRoot().toString();
    }

    public static JobVertexFlameGraph empty() {
        return new JobVertexFlameGraph(-1, null);
    }

    /** Graph node. */
    public static class Node {

        // These field names are required by the library used in the WebUI.
        private static final String FIELD_NAME_NAME = "name";
        private static final String FIELD_NAME_VALUE = "value";
        private static final String FIELD_NAME_CHILDREN = "children";

        @JsonProperty(FIELD_NAME_NAME)
        private final String stackTraceLocation;

        @JsonProperty(FIELD_NAME_VALUE)
        private final int hitCount;

        @JsonProperty(FIELD_NAME_CHILDREN)
        private final List<Node> children;

        @JsonCreator
        Node(
                @JsonProperty(FIELD_NAME_NAME) String stackTraceLocation,
                @JsonProperty(FIELD_NAME_VALUE) int hitCount,
                @JsonProperty(FIELD_NAME_CHILDREN) List<Node> children) {
            this.stackTraceLocation = stackTraceLocation;
            this.hitCount = hitCount;
            this.children = children;
        }

        @JsonIgnore
        public String getStackTraceLocation() {
            return stackTraceLocation;
        }

        @JsonIgnore
        public int getHitCount() {
            return hitCount;
        }

        @JsonIgnore
        public List<Node> getChildren() {
            return children;
        }

        @Override
        public String toString() {
            return getStackTraceLocation()
                    + ": "
                    + getHitCount()
                    + "\n"
                    + "\t"
                    + toStringChildren();
        }

        private String toStringChildren() {
            StringBuilder sb = new StringBuilder();
            for (Node child : getChildren()) {
                sb.append(child.toString());
            }
            return sb.toString();
        }
    }
}
