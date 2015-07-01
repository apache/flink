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
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashSet;

public class PathExistenceData {

	public static final Integer MAX_ITERATIONS = 4;

	public static final String EDGES = "1,2,12\n" + "3,2,32\n" + "3,1,31\n" + "4,3,43\n";
	public static final String VERTICES = "1\n" + "2\n" + "3\n" + "4\n";

	public static DataSet<Tuple3<Long, Long, Long>> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		DataSet<Tuple3<Long, Long, Long>> edges = env.fromElements(new Tuple3<Long, Long, Long>(1L,2L,12L) ,
				new Tuple3<Long, Long, Long>(3L,2L,32L) ,
				new Tuple3<Long, Long, Long>(3L,1L,31L),
				new Tuple3<Long, Long, Long>(4L, 3L, 43L)
		);
		return edges;
	}

	public static DataSet<Tuple2<Long, HashSet<Long>>> getDefaultVertexDataSet(ExecutionEnvironment env) {
		HashSet<Long> h1 = new HashSet<Long>() {{
			add(1L);
		}};
		HashSet<Long> h2 = new HashSet<Long>() {{
			add(2L);
		}};
		HashSet<Long> h3 = new HashSet<Long>() {{
			add(3L);
		}};
		HashSet<Long> h4 = new HashSet<Long>() {{
			add(4L);
		}};
		DataSet<Tuple2<Long, HashSet<Long>>> vertices = env.fromElements(new Tuple2<Long,HashSet<Long>> (1L, h1),
																		new Tuple2<Long,HashSet<Long>> (2L, h2),
																		new Tuple2<Long,HashSet<Long>> (3L, h3),
																		new Tuple2<Long,HashSet<Long>> (4L, h4));return vertices;
	}

	private PathExistenceData() {}

	public static final String RESULTINGVERTICES = "1,[1, 2]\n"
												+"4,[1, 2, 3, 4]\n"
												+"2,[2]\n"
												+"3,[1, 2, 3]";
}
