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

import java.util.LinkedList;
import java.util.List;

/**
 * Provides the default data set used for the Single Source Shortest Paths example program. If no
 * parameters are given to the program, the default edge data set is used.
 */
public class SingleSourceShortestPathsData {

    public static final Long SRC_VERTEX_ID = 1L;

    public static final String EDGES =
            "1\t2\t12.0\n"
                    + "1\t3\t13.0\n"
                    + "2\t3\t23.0\n"
                    + "3\t4\t34.0\n"
                    + "3\t5\t35.0\n"
                    + "4\t5\t45.0\n"
                    + "5\t1\t51.0";

    public static final Object[][] DEFAULT_EDGES =
            new Object[][] {
                new Object[] {1L, 2L, 12.0},
                new Object[] {1L, 3L, 13.0},
                new Object[] {2L, 3L, 23.0},
                new Object[] {3L, 4L, 34.0},
                new Object[] {3L, 5L, 35.0},
                new Object[] {4L, 5L, 45.0},
                new Object[] {5L, 1L, 51.0}
            };

    public static final String RESULTED_SINGLE_SOURCE_SHORTEST_PATHS =
            "1,0.0\n" + "2,12.0\n" + "3,13.0\n" + "4,47.0\n" + "5,48.0";

    public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Edge<Long, Double>> edgeList = new LinkedList<>();
        for (Object[] edge : DEFAULT_EDGES) {
            edgeList.add(new Edge<>((Long) edge[0], (Long) edge[1], (Double) edge[2]));
        }
        return env.fromCollection(edgeList);
    }

    private SingleSourceShortestPathsData() {}
}
