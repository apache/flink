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

package org.apache.flink.graph.pregel;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.IterationConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * A VertexCentricConfiguration object can be used to set the iteration name and
 * degree of parallelism, to register aggregators and use broadcast sets in
 * the {@link org.apache.flink.graph.pregel.ComputeFunction}.
 *
 * <p>The VertexCentricConfiguration object is passed as an argument to
 * {@link org.apache.flink.graph.Graph#runVertexCentricIteration (
 * org.apache.flink.graph.pregel.ComputeFunction, int, VertexCentricConfiguration)}.
 */
public class VertexCentricConfiguration extends IterationConfiguration {

	/** The broadcast variables for the compute function. **/
	private List<Tuple2<String, DataSet<?>>> bcVars = new ArrayList<>();

	public VertexCentricConfiguration() {}

	/**
	 * Adds a data set as a broadcast set to the compute function.
	 *
	 * @param name The name under which the broadcast data set is available in the compute function.
	 * @param data The data set to be broadcast.
	 */
	public void addBroadcastSet(String name, DataSet<?> data) {
		this.bcVars.add(new Tuple2<>(name, data));
	}

	/**
	 * Get the broadcast variables of the compute function.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast data set.
	 */
	public List<Tuple2<String, DataSet<?>>> getBcastVars() {
		return this.bcVars;
	}

}
