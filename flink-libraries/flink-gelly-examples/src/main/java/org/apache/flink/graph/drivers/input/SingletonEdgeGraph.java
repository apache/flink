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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import static org.apache.flink.graph.generator.PathGraph.MINIMUM_VERTEX_COUNT;

/**
 * Generate a {@link org.apache.flink.graph.generator.SingletonEdgeGraph}.
 */
public class SingletonEdgeGraph
extends GeneratedGraph {

	private LongParameter vertexPairCount = new LongParameter(this, "vertex_pair_count")
		.setMinimumValue(MINIMUM_VERTEX_COUNT)
		.setMaximumValue(1L << 62);

	@Override
	public String getIdentity() {
		return getName() + " (" + vertexPairCount + ")";
	}

	@Override
	protected long vertexCount() {
		return 2 * vertexPairCount.getValue();
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> create(ExecutionEnvironment env) {
		return new org.apache.flink.graph.generator.SingletonEdgeGraph(env, vertexPairCount.getValue())
			.setParallelism(parallelism.getValue().intValue())
			.generate();
	}
}
