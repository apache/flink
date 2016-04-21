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

package org.apache.flink.graph.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.Utils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.AbstractID;

public class GraphUtils {

	/**
	 * Convenience method to get the count (number of elements) of a Graph
	 * as well as the checksum (sum over element hashes). The vertex and
	 * edge DataSets are processed in a single job and the resultant counts
	 * and checksums are merged locally.
	 *
	 * @param graph Graph over which to compute the count and checksum
	 * @return the checksum over the vertices and edges
	 */
	public static <K, VV, EV> Utils.ChecksumHashCode checksumHashCode(Graph<K, VV, EV> graph) throws Exception {
		final String verticesId = new AbstractID().toString();
		graph.getVertices().output(new Utils.ChecksumHashCodeHelper<Vertex<K, VV>>(verticesId)).name("ChecksumHashCode vertices");

		final String edgesId = new AbstractID().toString();
		graph.getEdges().output(new Utils.ChecksumHashCodeHelper<Edge<K, EV>>(edgesId)).name("ChecksumHashCode edges");

		JobExecutionResult res = graph.getContext().execute();

		Utils.ChecksumHashCode checksum = res.<Utils.ChecksumHashCode>getAccumulatorResult(verticesId);
		checksum.add(res.<Utils.ChecksumHashCode>getAccumulatorResult(edgesId));

		return checksum;
	}
}
