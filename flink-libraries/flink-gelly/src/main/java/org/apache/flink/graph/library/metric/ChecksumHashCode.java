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

package org.apache.flink.graph.library.metric;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.Utils;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.AbstractID;

/**
 * Convenience method to get the count (number of elements) of a Graph
 * as well as the checksum (sum over element hashes). The vertex and
 * edge DataSets are processed in a single job and the resultant counts
 * and checksums are merged locally.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class ChecksumHashCode<K, VV, EV>
extends AbstractGraphAnalytic<K, VV, EV, Utils.ChecksumHashCode> {

	private String verticesId = new AbstractID().toString();

	private String edgesId = new AbstractID().toString();

	@Override
	public ChecksumHashCode<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		input
			.getVertices()
			.output(new Utils.ChecksumHashCodeHelper<Vertex<K, VV>>(verticesId))
				.name("ChecksumHashCode vertices");

		input
			.getEdges()
			.output(new Utils.ChecksumHashCodeHelper<Edge<K, EV>>(edgesId))
				.name("ChecksumHashCode edges");

		return this;
	}

	@Override
	public Utils.ChecksumHashCode getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();
		Utils.ChecksumHashCode checksum = res.getAccumulatorResult(verticesId);
		checksum.add(res.<Utils.ChecksumHashCode>getAccumulatorResult(edgesId));
		return checksum;
	}
}
