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
 * Provides the default data set used for the Simple Community Detection example program.
 * If no parameters are given to the program, the default edge data set is used.
 */
public class CommunityDetectionData {

	// the algorithm is not guaranteed to always converge
	public static final Integer MAX_ITERATIONS = 30;

	public static final double DELTA = 0.5f;

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, 1.0));
		edges.add(new Edge<Long, Double>(1L, 3L, 2.0));
		edges.add(new Edge<Long, Double>(1L, 4L, 3.0));
		edges.add(new Edge<Long, Double>(2L, 3L, 4.0));
		edges.add(new Edge<Long, Double>(2L, 4L, 5.0));
		edges.add(new Edge<Long, Double>(3L, 5L, 6.0));
		edges.add(new Edge<Long, Double>(5L, 6L, 7.0));
		edges.add(new Edge<Long, Double>(5L, 7L, 8.0));
		edges.add(new Edge<Long, Double>(6L, 7L, 9.0));
		edges.add(new Edge<Long, Double>(7L, 12L, 10.0));
		edges.add(new Edge<Long, Double>(8L, 9L, 11.0));
		edges.add(new Edge<Long, Double>(8L, 10L, 12.0));
		edges.add(new Edge<Long, Double>(8L, 11L, 13.0));
		edges.add(new Edge<Long, Double>(9L, 10L, 14.0));
		edges.add(new Edge<Long, Double>(9L, 11L, 15.0));
		edges.add(new Edge<Long, Double>(10L, 11L, 16.0));
		edges.add(new Edge<Long, Double>(10L, 12L, 17.0));
		edges.add(new Edge<Long, Double>(11L, 12L, 18.0));

		return env.fromCollection(edges);
	}

	private CommunityDetectionData() {}
}
