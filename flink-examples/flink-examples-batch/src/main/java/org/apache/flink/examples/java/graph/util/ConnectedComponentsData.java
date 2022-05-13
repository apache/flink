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

package org.apache.flink.examples.java.graph.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;
import java.util.List;

/**
 * Provides the default data sets used for the Connected Components example program. The default
 * data sets are used, if no parameters are given to the program.
 */
public class ConnectedComponentsData {

    public static final long[] VERTICES =
            new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    public static DataSet<Long> getDefaultVertexDataSet(ExecutionEnvironment env) {
        List<Long> verticesList = new LinkedList<Long>();
        for (long vertexId : VERTICES) {
            verticesList.add(vertexId);
        }
        return env.fromCollection(verticesList);
    }

    public static final Object[][] EDGES =
            new Object[][] {
                new Object[] {1L, 2L},
                new Object[] {2L, 3L},
                new Object[] {2L, 4L},
                new Object[] {3L, 5L},
                new Object[] {6L, 7L},
                new Object[] {8L, 9L},
                new Object[] {8L, 10L},
                new Object[] {5L, 11L},
                new Object[] {11L, 12L},
                new Object[] {10L, 13L},
                new Object[] {9L, 14L},
                new Object[] {13L, 14L},
                new Object[] {1L, 15L},
                new Object[] {16L, 1L}
            };

    public static DataSet<Tuple2<Long, Long>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Tuple2<Long, Long>> edgeList = new LinkedList<Tuple2<Long, Long>>();
        for (Object[] edge : EDGES) {
            edgeList.add(new Tuple2<Long, Long>((Long) edge[0], (Long) edge[1]));
        }
        return env.fromCollection(edgeList);
    }
}
