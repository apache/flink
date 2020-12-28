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
import org.apache.flink.examples.java.graph.util.EnumTrianglesDataTypes.Edge;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Triangle Enumeration example programs. The default
 * data sets are used, if no parameters are given to the program.
 */
public class EnumTrianglesData {

    public static final Object[][] EDGES = {
        {1, 2},
        {1, 3},
        {1, 4},
        {1, 5},
        {2, 3},
        {2, 5},
        {3, 4},
        {3, 7},
        {3, 8},
        {5, 6},
        {7, 8}
    };

    public static DataSet<EnumTrianglesDataTypes.Edge> getDefaultEdgeDataSet(
            ExecutionEnvironment env) {

        List<EnumTrianglesDataTypes.Edge> edges = new ArrayList<EnumTrianglesDataTypes.Edge>();
        for (Object[] e : EDGES) {
            edges.add(new Edge((Integer) e[0], (Integer) e[1]));
        }

        return env.fromCollection(edges);
    }
}
