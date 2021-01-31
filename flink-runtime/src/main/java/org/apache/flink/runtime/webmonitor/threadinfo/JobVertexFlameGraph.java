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

import org.apache.flink.runtime.webmonitor.stats.Statistics;

import java.io.Serializable;
import java.util.List;

/**
 * Flame Graph representation for a job vertex.
 *
 * <p>Statistics are gathered by sampling stack traces of running tasks.
 */
public class JobVertexFlameGraph implements Serializable, Statistics {

    private static final long serialVersionUID = 1L;

    /** End time stamp of the corresponding sample. */
    private final long endTimestamp;

    private final Node root;

    public JobVertexFlameGraph(long endTimestamp, Node root) {
        this.endTimestamp = endTimestamp;
        this.root = root;
    }

    public long getEndTime() {
        return endTimestamp;
    }

    public Node getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "OperatorFlameGraph: endTimestamp=" + endTimestamp + "\n" + getRoot().toString();
    }

    /** Graph node. */
    public static class Node {

        private final String name;
        private final int value;
        private final List<Node> children;

        Node(String name, int value, List<Node> children) {
            this.name = name;
            this.value = value;
            this.children = children;
        }

        public String getName() {
            return name;
        }

        public int getValue() {
            return value;
        }

        public List<Node> getChildren() {
            return children;
        }

        @Override
        public String toString() {
            return getName() + ": " + getValue() + "\n" + "\t" + toStringChildren();
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
