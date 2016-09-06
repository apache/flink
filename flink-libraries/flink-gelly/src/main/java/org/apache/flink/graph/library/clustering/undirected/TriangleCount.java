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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.CountHelper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.AbstractID;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Count the number of distinct triangles in an undirected graph.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @see TriangleListing
 */
public class TriangleCount<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends AbstractGraphAnalytic<K, VV, EV, Long> {

	private String id = new AbstractID().toString();

	// Optional configuration
	private int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public TriangleCount<K, VV, EV> setLittleParallelism(int littleParallelism) {
		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	public TriangleCount<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		DataSet<Tuple3<K, K, K>> triangles = input
			.run(new TriangleListing<K, VV, EV>()
				.setSortTriangleVertices(false)
				.setLittleParallelism(littleParallelism));

		triangles
			.output(new CountHelper<Tuple3<K, K, K>>(id))
				.name("Count triangles");

		return this;
	}

	@Override
	public Long getResult() {
		return env.getLastJobExecutionResult().<Long> getAccumulatorResult(id);
	}
}
