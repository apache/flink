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

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;

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
extends GraphAnalyticBase<K, VV, EV, Checksum> {

	private org.apache.flink.graph.asm.dataset.ChecksumHashCode<Vertex<K, VV>> vertexChecksum;

	private org.apache.flink.graph.asm.dataset.ChecksumHashCode<Edge<K, EV>> edgeChecksum;

	@Override
	public ChecksumHashCode<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		vertexChecksum = new org.apache.flink.graph.asm.dataset.ChecksumHashCode<>();
		vertexChecksum.run(input.getVertices());

		edgeChecksum = new org.apache.flink.graph.asm.dataset.ChecksumHashCode<>();
		edgeChecksum.run(input.getEdges());

		return this;
	}

	@Override
	public Checksum getResult() {
		Checksum checksum = vertexChecksum.getResult();
		checksum.add(edgeChecksum.getResult());
		return checksum;
	}
}
