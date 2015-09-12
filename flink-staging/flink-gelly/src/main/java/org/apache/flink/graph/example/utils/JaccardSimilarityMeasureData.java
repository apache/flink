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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.HashSet;
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

	public static DataSet<Edge<String, NullValue>> getDefaultStringNullValueEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<String, NullValue>> edges = new ArrayList<Edge<String, NullValue>>();
		edges.add(new Edge<String, NullValue>("1", "2", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("4", "5", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String JACCARD_EDGES = "1,2,0.6\n" + "1,3,0.6\n" + "1,4,0.6\n" + "1,5,0.6\n" +
			"2,3,0.6\n" + "2,4,0.6\n" + "2,5,0.6\n" + "3,4,0.6\n" + "3,5,0.6\n" + "4,5,0.6";

	public static final Integer ALPHA = 2;

	public static final Integer LEVEL = 4;

	public static final Integer THRESHOLD = 2;

	public static DataSet<Edge<String, NullValue>> getEdgesBeforeBothSteps (ExecutionEnvironment env) {

		List<Edge<String, NullValue>> edges = new ArrayList<Edge<String, NullValue>>();
		edges.add(new Edge<String, NullValue>("2_1","3_0", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3_0","4_0", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1_0","4_0", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1_0","2_0", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1_1","3_1", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2_0","4_1", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1_1","5_1", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3_1","5_1", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2_1","5_0", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("4_1","5_0", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> getVerticesBeforeStepOne (ExecutionEnvironment env) {

		List<Vertex<String, Tuple2<String, HashSet<String>>>> vertices =
				new ArrayList<Vertex<String, Tuple2<String, HashSet<String>>>>();

		HashSet<String> one = new HashSet<String>(); one.add("1");
		HashSet<String> two = new HashSet<String>(); two.add("2");
		HashSet<String> three = new HashSet<String>(); three.add("3");
		HashSet<String> four = new HashSet<String>(); four.add("4");
		HashSet<String> five = new HashSet<String>(); five.add("5");

		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("3_0",
				new Tuple2<String, HashSet<String>>("3", three)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("4_0",
				new Tuple2<String, HashSet<String>>("4", four)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("1_1",
				new Tuple2<String, HashSet<String>>("1", one)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("2_0",
				new Tuple2<String, HashSet<String>>("2", two)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("2_1",
				new Tuple2<String, HashSet<String>>("2", two)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("3_1",
				new Tuple2<String, HashSet<String>>("3", three)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("4_1",
				new Tuple2<String, HashSet<String>>("4", four)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("5_1",
				new Tuple2<String, HashSet<String>>("5", five)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("1_0",
				new Tuple2<String, HashSet<String>>("1", one)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("5_0",
				new Tuple2<String, HashSet<String>>("5", five)));

		return env.fromCollection(vertices);
	}

	public static final String RESULT_AFTER_STEP_ONE = "1_0,(1,[2, 3, 4, 5])\n" + "1_1,(1,[2, 3, 4, 5])\n" +
			"2_0,(2,[1, 3, 4, 5])\n" + "2_1,(2,[1, 3, 4, 5])\n" + "4_0,(4,[1, 2, 3, 5])\n" +
			"4_1,(4,[1, 2, 3, 5])\n" + "5_0,(5,[1, 2, 3, 4])\n" + "5_1,(5,[1, 2, 3, 4])\n" +
			"3_0,(3,[1, 2, 4, 5])\n" + "3_1,(3,[1, 2, 4, 5])";

	public static DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> getVerticesBeforeStepTwo (ExecutionEnvironment env) {

		List<Vertex<String, Tuple2<String, HashSet<String>>>> vertices =
				new ArrayList<Vertex<String, Tuple2<String, HashSet<String>>>>();

		HashSet<String> one = new HashSet<String>(); one.add("3"); one.add("2"); one.add("5"); one.add("4");
		HashSet<String> two = new HashSet<String>(); two.add("3"); two.add("1"); two.add("5"); two.add("4");
		HashSet<String> three = new HashSet<String>(); three.add("2"); three.add("1"); three.add("5"); three.add("4");
		HashSet<String> four = new HashSet<String>(); four.add("3"); four.add("2"); four.add("1"); four.add("5");
		HashSet<String> five = new HashSet<String>(); five.add("3"); five.add("2"); five.add("1"); five.add("4");

		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("3_0",
				new Tuple2<String, HashSet<String>>("3", three)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("4_0",
				new Tuple2<String, HashSet<String>>("4", four)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("1_1",
				new Tuple2<String, HashSet<String>>("1", one)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("2_0",
				new Tuple2<String, HashSet<String>>("2", two)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("2_1",
				new Tuple2<String, HashSet<String>>("2", two)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("3_1",
				new Tuple2<String, HashSet<String>>("3", three)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("4_1",
				new Tuple2<String, HashSet<String>>("4", four)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("5_1",
				new Tuple2<String, HashSet<String>>("5", five)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("1_0",
				new Tuple2<String, HashSet<String>>("1", one)));
		vertices.add(new Vertex<String, Tuple2<String, HashSet<String>>>("5_0",
				new Tuple2<String, HashSet<String>>("5", five)));

		return env.fromCollection(vertices);
	}

	private JaccardSimilarityMeasureData() {}
}
