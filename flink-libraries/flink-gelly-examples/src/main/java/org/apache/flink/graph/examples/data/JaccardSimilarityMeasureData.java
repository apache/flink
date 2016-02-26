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

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Jaccard Similarity Measure example program.
 * If no parameters are given to the program, the default data sets are used.
 */
public class JaccardSimilarityMeasureData {

	public static final String EDGES = "1	2\n" + "1	3\n" + "1	4\n" + "1	5\n" + "2	3\n" + "2	4\n" +
			"2	5\n" + "3	4\n" + "3	5\n" + "4	5";

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 3L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 3L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(3L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(3L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(4L, 5L, new Double(0)));

		return env.fromCollection(edges);
	}

	public static final String JACCARD_EDGES = "1,2,0.6\n" + "1,3,0.6\n" + "1,4,0.6\n" + "1,5,0.6\n" +
			"2,3,0.6\n" + "2,4,0.6\n" + "2,5,0.6\n" + "3,4,0.6\n" + "3,5,0.6\n" + "4,5,0.6";

	private JaccardSimilarityMeasureData() {}
}
