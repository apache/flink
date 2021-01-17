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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.Simplify.Ordering;
import org.apache.flink.types.NullValue;

/**
 * A simple graph has no self-loops (edges where the source and target vertices are the same) and no
 * duplicate edges. Flink stores an undirected graph as a directed graph where each undirected edge
 * is represented by a directed edge in each direction.
 *
 * <p>This {@link Parameter} indicates whether to simplify the graph and if the graph should be
 * directed or undirected.
 */
public class Simplify implements Parameter<Ordering> {

    /** Whether and how to simplify the graph. */
    public enum Ordering {
        // leave the graph unchanged
        NONE,

        // create a simple, directed graph
        DIRECTED,

        // create a simple, undirected graph
        UNDIRECTED,

        // create a simple, undirected graph
        // remove input edges where source < target before symmetrizing the graph
        UNDIRECTED_CLIP_AND_FLIP,
    }

    private Ordering value;

    /**
     * Add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     */
    public Simplify(ParameterizedBase owner) {
        owner.addParameter(this);
    }

    @Override
    public String getUsage() {
        return "[--simplify <directed | undirected [--clip_and_flip]>] ";
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public void configure(ParameterTool parameterTool) {
        String ordering = parameterTool.get("simplify");

        if (ordering == null) {
            value = Ordering.NONE;
        } else {
            switch (ordering.toLowerCase()) {
                case "directed":
                    value = Ordering.DIRECTED;
                    break;
                case "undirected":
                    value =
                            parameterTool.has("clip_and_flip")
                                    ? Ordering.UNDIRECTED_CLIP_AND_FLIP
                                    : Ordering.UNDIRECTED;
                    break;
                default:
                    throw new ProgramParametrizationException(
                            "Expected 'directed' or 'undirected' ordering but received '"
                                    + ordering
                                    + "'");
            }
        }
    }

    @Override
    public Ordering getValue() {
        return value;
    }

    /**
     * Simplify the given graph based on the configured value.
     *
     * @param graph input graph
     * @param <T> graph key type
     * @return output graph
     * @throws Exception on error
     */
    public <T extends Comparable<T>> Graph<T, NullValue, NullValue> simplify(
            Graph<T, NullValue, NullValue> graph, int parallelism) throws Exception {
        switch (value) {
            case DIRECTED:
                graph =
                        graph.run(
                                new org.apache.flink.graph.asm.simple.directed.Simplify<
                                                T, NullValue, NullValue>()
                                        .setParallelism(parallelism));
                break;
            case UNDIRECTED:
                graph =
                        graph.run(
                                new org.apache.flink.graph.asm.simple.undirected.Simplify<
                                                T, NullValue, NullValue>(false)
                                        .setParallelism(parallelism));
                break;
            case UNDIRECTED_CLIP_AND_FLIP:
                graph =
                        graph.run(
                                new org.apache.flink.graph.asm.simple.undirected.Simplify<
                                                T, NullValue, NullValue>(true)
                                        .setParallelism(parallelism));
                break;
        }

        return graph;
    }

    public String getShortString() {
        switch (value) {
            case DIRECTED:
                return "d";
            case UNDIRECTED:
                return "u";
            case UNDIRECTED_CLIP_AND_FLIP:
                return "ɔ";
            default:
                return "";
        }
    }
}
