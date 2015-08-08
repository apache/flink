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

package org.apache.flink.graph.example.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Adamic Adar Similarity Measure example program.
 * If no parameters are given to the program, the default data sets are used.
 */
public class AdamicAdarSimilarityMeasureData {

	public static final String EDGES = "1	2\n" + "3	2\n" + "4	2\n" + "5	3\n" + "5	1\n" + "3	1\n";

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, 0.0));
		edges.add(new Edge<Long, Double>(4L, 2L, 0.0));
		edges.add(new Edge<Long, Double>(3L, 2L, 0.0));
		edges.add(new Edge<Long, Double>(5L, 3L, 0.0));
		edges.add(new Edge<Long, Double>(5L, 1L, 0.0));
		edges.add(new Edge<Long, Double>(3L, 1L, 0.0));
		
		return env.fromCollection(edges);
	}

	public static final String ADAMIC_EDGES = "4,2,0.0\n"
			+"1,2,0.9102392266268373\n"
			+"3,2,0.9102392266268373\n"
			+"3,1,2.352934267515801\n"
			+"5,1,0.9102392266268373\n"
			+"5,3,0.9102392266268373\n";

	private AdamicAdarSimilarityMeasureData() {}
}
