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

package org.apache.flink.graph.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import java.util.ArrayList;
import java.util.List;

/** Provides the default data set used for Summarization tests. */
public class SummarizationData {

    private SummarizationData() {}

    /**
     * Vertices of the input graph.
     *
     * <p>Format:
     *
     * <p>"vertex-id;vertex-value"
     */
    private static final String[] INPUT_VERTICES =
            new String[] {"0;1", "1;1", "2;2", "3;2", "4;2", "5;3"};

    /**
     * Edges of the input graph.
     *
     * <p>Format:
     *
     * <p>"source-id;target-id;edge-value
     */
    private static final String[] INPUT_EDGES =
            new String[] {
                "0;1;1", "1;0;1", "1;2;1", "2;1;1", "2;3;2", "3;2;2", "4;0;3", "4;1;3", "5;2;4",
                "5;3;4"
            };

    /**
     * The resulting vertex id can be any id of the vertices summarized by the single vertex.
     *
     * <p>Format:
     *
     * <p>"possible-id[,possible-id];group-value,group-count"
     */
    public static final String[] EXPECTED_VERTICES = new String[] {"0,1;1,2", "2,3,4;2,3", "5;3,1"};

    /**
     * The expected output from the input edges.
     *
     * <p>Format:
     *
     * <p>"possible-source-id[,possible-source-id];possible-target-id[,possible-target-id];group-value,group-count"
     */
    public static final String[] EXPECTED_EDGES_WITH_VALUES =
            new String[] {
                "0,1;0,1;1,2",
                "0,1;2,3,4;1,1",
                "2,3,4;0,1;1,1",
                "2,3,4;0,1;3,2",
                "2,3,4;2,3,4;2,2",
                "5;2,3,4;4,2"
            };

    /**
     * The expected output from the input edges translated to null values.
     *
     * <p>Format:
     *
     * <p>"possible-source-id[,possible-source-id];possible-target-id[,possible-target-id];group-value,group-count"
     */
    public static final String[] EXPECTED_EDGES_ABSENT_VALUES =
            new String[] {
                "0,1;0,1;(null),2",
                "0,1;2,3,4;(null),1",
                "2,3,4;0,1;(null),3",
                "2,3,4;2,3,4;(null),2",
                "5;2,3,4;(null),2"
            };

    /**
     * Creates a set of vertices with attached {@link String} values.
     *
     * @param env execution environment
     * @return vertex data set with string values
     */
    public static DataSet<Vertex<Long, String>> getVertices(ExecutionEnvironment env) {
        List<Vertex<Long, String>> vertices = new ArrayList<>(INPUT_VERTICES.length);
        for (String vertex : INPUT_VERTICES) {
            String[] tokens = vertex.split(";");
            vertices.add(new Vertex<>(Long.parseLong(tokens[0]), tokens[1]));
        }

        return env.fromCollection(vertices);
    }

    /**
     * Creates a set of edges with attached {@link String} values.
     *
     * @param env execution environment
     * @return edge data set with string values
     */
    public static DataSet<Edge<Long, String>> getEdges(ExecutionEnvironment env) {
        List<Edge<Long, String>> edges = new ArrayList<>(INPUT_EDGES.length);
        for (String edge : INPUT_EDGES) {
            String[] tokens = edge.split(";");
            edges.add(new Edge<>(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]), tokens[2]));
        }

        return env.fromCollection(edges);
    }
}
