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
import org.apache.flink.graph.examples.EuclideanGraphWeighing;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Euclidean Graph example program. If no parameters are
 * given to the program, the default data sets are used.
 */
public class EuclideanGraphData {

    public static final int NUM_VERTICES = 9;

    public static final String VERTICES =
            "1,1.0,1.0\n"
                    + "2,2.0,2.0\n"
                    + "3,3.0,3.0\n"
                    + "4,4.0,4.0\n"
                    + "5,5.0,5.0\n"
                    + "6,6.0,6.0\n"
                    + "7,7.0,7.0\n"
                    + "8,8.0,8.0\n"
                    + "9,9.0,9.0";

    public static DataSet<Vertex<Long, EuclideanGraphWeighing.Point>> getDefaultVertexDataSet(
            ExecutionEnvironment env) {

        List<Vertex<Long, EuclideanGraphWeighing.Point>> vertices = new ArrayList<>();
        for (int i = 1; i <= NUM_VERTICES; i++) {
            vertices.add(
                    new Vertex<>(
                            new Long(i),
                            new EuclideanGraphWeighing.Point(new Double(i), new Double(i))));
        }

        return env.fromCollection(vertices);
    }

    public static final String EDGES =
            "1,2\n" + "1,4\n" + "2,3\n" + "2,4\n" + "2,5\n" + "3,5\n" + "4,5\n" + "4,6\n" + "5,7\n"
                    + "5,9\n" + "6,7\n" + "6,8\n" + "7,8\n" + "7,9\n" + "8,9";

    public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Edge<Long, Double>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 2L, 0.0));
        edges.add(new Edge<>(1L, 4L, 0.0));
        edges.add(new Edge<>(2L, 3L, 0.0));
        edges.add(new Edge<>(2L, 4L, 0.0));
        edges.add(new Edge<>(2L, 5L, 0.0));
        edges.add(new Edge<>(3L, 5L, 0.0));
        edges.add(new Edge<>(4L, 5L, 0.0));
        edges.add(new Edge<>(4L, 6L, 0.0));
        edges.add(new Edge<>(5L, 7L, 0.0));
        edges.add(new Edge<>(5L, 9L, 0.0));
        edges.add(new Edge<>(6L, 7L, 0.0));
        edges.add(new Edge<>(6L, 8L, 0.0));
        edges.add(new Edge<>(6L, 8L, 0.0));
        edges.add(new Edge<>(7L, 8L, 0.0));
        edges.add(new Edge<>(7L, 9L, 0.0));
        edges.add(new Edge<>(8L, 9L, 0.0));

        return env.fromCollection(edges);
    }

    public static final String RESULTED_WEIGHTED_EDGES =
            "1,2,1.4142135623730951\n"
                    + "1,4,4.242640687119285\n"
                    + "2,3,1.4142135623730951\n"
                    + "2,4,2.8284271247461903\n"
                    + "2,5,4.242640687119285\n"
                    + "3,5,2.8284271247461903\n"
                    + "4,5,1.4142135623730951\n"
                    + "4,6,2.8284271247461903\n"
                    + "5,7,2.8284271247461903\n"
                    + "5,9,5.656854249492381\n"
                    + "6,7,1.4142135623730951\n"
                    + "6,8,2.8284271247461903\n"
                    + "7,8,1.4142135623730951\n"
                    + "7,9,2.8284271247461903\n"
                    + "8,9,1.4142135623730951";

    private EuclideanGraphData() {}
}
