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

/**
 * Provides the default data sets used for the IncrementalSSSP example program. If no parameters are
 * given to the program, the default data sets are used.
 */
public class IncrementalSSSPData {

    public static final int NUM_VERTICES = 5;

    public static final String VERTICES = "1,6.0\n" + "2,2.0\n" + "3,3.0\n" + "4,1.0\n" + "5,0.0";

    public static DataSet<Vertex<Long, Double>> getDefaultVertexDataSet(ExecutionEnvironment env) {

        List<Vertex<Long, Double>> vertices = new ArrayList<>();
        vertices.add(new Vertex<>(1L, 6.0));
        vertices.add(new Vertex<>(2L, 2.0));
        vertices.add(new Vertex<>(3L, 3.0));
        vertices.add(new Vertex<>(4L, 1.0));
        vertices.add(new Vertex<>(5L, 0.0));

        return env.fromCollection(vertices);
    }

    public static final String EDGES =
            "1,3,3.0\n" + "2,4,3.0\n" + "2,5,2.0\n" + "3,2,1.0\n" + "3,5,5.0\n" + "4,5,1.0";

    public static final DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(
            ExecutionEnvironment env) {

        List<Edge<Long, Double>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 3L, 3.0));
        edges.add(new Edge<>(2L, 4L, 3.0));
        edges.add(new Edge<>(2L, 5L, 2.0));
        edges.add(new Edge<>(3L, 2L, 1.0));
        edges.add(new Edge<>(3L, 5L, 5.0));
        edges.add(new Edge<>(4L, 5L, 1.0));

        return env.fromCollection(edges);
    }

    public static final String EDGES_IN_SSSP = "1,3,3.0\n" + "2,5,2.0\n" + "3,2,1.0\n" + "4,5,1.0";

    public static final DataSet<Edge<Long, Double>> getDefaultEdgesInSSSP(
            ExecutionEnvironment env) {

        List<Edge<Long, Double>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 3L, 3.0));
        edges.add(new Edge<>(2L, 5L, 2.0));
        edges.add(new Edge<>(3L, 2L, 1.0));
        edges.add(new Edge<>(4L, 5L, 1.0));

        return env.fromCollection(edges);
    }

    public static final String SRC_EDGE_TO_BE_REMOVED = "2";

    public static final String TRG_EDGE_TO_BE_REMOVED = "5";

    public static final String VAL_EDGE_TO_BE_REMOVED = "2.0";

    public static final Edge<Long, Double> getDefaultEdgeToBeRemoved() {

        return new Edge<>(2L, 5L, 2.0);
    }

    public static final String RESULTED_VERTICES =
            "1,"
                    + Double.MAX_VALUE
                    + "\n"
                    + "2,"
                    + Double.MAX_VALUE
                    + "\n"
                    + "3,"
                    + Double.MAX_VALUE
                    + "\n"
                    + "4,1.0\n"
                    + "5,0.0";

    private IncrementalSSSPData() {}
}
