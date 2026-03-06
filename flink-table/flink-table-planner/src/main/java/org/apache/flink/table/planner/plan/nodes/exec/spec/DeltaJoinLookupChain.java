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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * An ordered chain of lookup operations used by the stream side to look up tables on the lookup
 * side in a delta join.
 *
 * <p>Each {@link Node} in the chain represents a single lookup step: using one or more already
 * resolved binary inputs (stream-side tables or previously looked-up tables) to look up another
 * binary input on the lookup side via a {@link DeltaJoinSpec}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeltaJoinLookupChain {

    private static final String FIELD_NAME_NODES = "nodes";

    @JsonProperty(FIELD_NAME_NODES)
    private final List<Node> nodes;

    private DeltaJoinLookupChain() {
        this(new ArrayList<>());
    }

    @JsonCreator
    public DeltaJoinLookupChain(@JsonProperty(FIELD_NAME_NODES) List<Node> nodes) {
        this.nodes = nodes;
    }

    public static DeltaJoinLookupChain newInstance() {
        return new DeltaJoinLookupChain();
    }

    @JsonIgnore
    public void addNode(Node node) {
        nodes.add(node);
    }

    @JsonIgnore
    public int size() {
        return nodes.size();
    }

    @JsonIgnore
    public List<Node> getNodes() {
        return ImmutableList.copyOf(nodes);
    }

    @JsonIgnore
    public Iterator<Node> newIterator() {
        return getNodes().iterator();
    }

    /** A single lookup step within the {@link DeltaJoinLookupChain}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Node {

        private static final String FIELD_NAME_INPUT_TABLE_BINARY_INPUT_ORDINALS =
                "inputTableBinaryInputOrdinals";
        private static final String FIELD_NAME_LOOKUP_TABLE_BINARY_INPUT_ORDINAL =
                "lookupTableBinaryInputOrdinal";
        private static final String FIELD_NAME_DELTA_JOIN_SPEC = "deltaJoinSpec";
        private static final String FIELD_NAME_JOIN_TYPE = "joinType";

        @JsonProperty(FIELD_NAME_INPUT_TABLE_BINARY_INPUT_ORDINALS)
        public final int[] inputTableBinaryInputOrdinals;

        @JsonProperty(FIELD_NAME_LOOKUP_TABLE_BINARY_INPUT_ORDINAL)
        public final int lookupTableBinaryInputOrdinal;

        // treat dest as lookup side
        @JsonProperty(FIELD_NAME_DELTA_JOIN_SPEC)
        public final DeltaJoinSpec deltaJoinSpec;

        @JsonProperty(FIELD_NAME_JOIN_TYPE)
        public final FlinkJoinType joinType;

        @JsonCreator
        public Node(
                @JsonProperty(FIELD_NAME_INPUT_TABLE_BINARY_INPUT_ORDINALS)
                        int[] inputTableBinaryInputOrdinals,
                @JsonProperty(FIELD_NAME_LOOKUP_TABLE_BINARY_INPUT_ORDINAL)
                        int lookupTableBinaryInputOrdinal,
                @JsonProperty(FIELD_NAME_DELTA_JOIN_SPEC) DeltaJoinSpec deltaJoinSpec,
                @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType) {
            this.inputTableBinaryInputOrdinals = inputTableBinaryInputOrdinals;
            this.lookupTableBinaryInputOrdinal = lookupTableBinaryInputOrdinal;
            this.deltaJoinSpec = deltaJoinSpec;
            this.joinType = joinType;

            Preconditions.checkArgument(DeltaJoinUtil.isJoinTypeSupported(joinType));

            Preconditions.checkArgument(inputTableBinaryInputOrdinals.length > 0);

            // check sorted and continuous
            for (int i = 1; i < inputTableBinaryInputOrdinals.length; i++) {
                Preconditions.checkArgument(
                        inputTableBinaryInputOrdinals[i]
                                == inputTableBinaryInputOrdinals[i - 1] + 1,
                        "The ordinals of binary input must be sorted and continuous, but are: "
                                + Arrays.toString(inputTableBinaryInputOrdinals));
            }
        }

        public static Node of(
                int[] inputTableBinaryInputOrdinals,
                int lookupTableBinaryInputOrdinal,
                DeltaJoinSpec deltaJoinSpec,
                FlinkJoinType joinType) {

            return new Node(
                    inputTableBinaryInputOrdinals,
                    lookupTableBinaryInputOrdinal,
                    deltaJoinSpec,
                    joinType);
        }

        public static Node of(
                int inputTableBinaryInputOrdinal,
                int lookupTableBinaryInputOrdinal,
                DeltaJoinSpec deltaJoinSpec,
                FlinkJoinType joinType) {
            return of(
                    new int[] {inputTableBinaryInputOrdinal},
                    lookupTableBinaryInputOrdinal,
                    deltaJoinSpec,
                    joinType);
        }
    }
}
