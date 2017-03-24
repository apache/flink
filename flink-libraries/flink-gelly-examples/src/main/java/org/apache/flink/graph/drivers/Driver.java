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

package org.apache.flink.graph.drivers;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.drivers.parameter.Parameterized;

/**
 * A driver for one or more {@link GraphAlgorithm}s and/or
 * {@link GraphAnalytic}s.
 *
 * It is preferable to include multiple, overlapping algorithms/analytics in
 * the same driver both for simplicity and since this examples module
 * demonstrates Flink capabilities rather than absolute performance.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public interface Driver<K, VV, EV>
extends Parameterized {

	/**
	 * A one-line description, presented in the algorithm listing.
	 *
	 * @return short description
	 */
	String getShortDescription();

	/**
	 * A multi-line description, presented in the algorithm usage.
	 *
	 * @return long description
	 */
	String getLongDescription();

	/**
	 * "Run" algorithms and analytics on the input graph. The execution plan
	 * is not finalized here but in the output methods.
	 *
	 * Drivers are first configured, next planned, and finally the chosen
	 * output method is called.
	 *
	 * @param graph input graph
	 * @throws Exception on error
	 */
	void plan(Graph<K, VV, EV> graph) throws Exception;
}
