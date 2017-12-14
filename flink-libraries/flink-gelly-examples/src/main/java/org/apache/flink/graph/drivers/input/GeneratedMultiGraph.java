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
import org.apache.flink.graph.drivers.parameter.Simplify;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Base class for graph generators which may create duplicate edges.
 *
 * @param <K> graph ID type
 */
public abstract class GeneratedMultiGraph<K extends Comparable<K>>
extends GeneratedGraph<LongValue> {

	private Simplify simplify = new Simplify(this);

	/**
	 * Get the short string representation of the simplify transformation.
	 *
	 * @return short string representation of the simplify transformation
	 */
	protected String getSimplifyShortString() {
		return simplify.getShortString();
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> create(ExecutionEnvironment env)
			throws Exception {
		Graph<LongValue, NullValue, NullValue> graph = generate(env);

		// simplify after the translation to improve the performance of the
		// simplify operators by processing smaller data types
		return simplify.simplify(graph, parallelism.getValue().intValue());
	}

	public abstract Graph<LongValue, NullValue, NullValue> generate(ExecutionEnvironment env) throws Exception;
}
