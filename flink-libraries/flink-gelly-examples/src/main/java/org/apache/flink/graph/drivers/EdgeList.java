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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;

import java.util.List;

/**
 * Convert a {@link Graph} to the {@link DataSet} of {@link Edge}s.
 */
public class EdgeList<K, VV, EV>
extends ParameterizedBase
implements Driver<K, VV, EV>, CSV, Hash, Print {

	private DataSet<Edge<K, EV>> edges;

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "the edge list";
	}

	@Override
	public String getLongDescription() {
		return "Pass-through of the graph's edge list.";
	}

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		edges = graph
			.getEdges();
	}

	@Override
	public void hash(String executionName) throws Exception {
		Checksum checksum = new ChecksumHashCode<Edge<K, EV>>()
			.run(edges)
			.execute(executionName);

		System.out.println(checksum);
	}

	@Override
	public void print(String executionName) throws Exception {
		Collect<Edge<K, EV>> collector = new Collect<>();

		// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
		List<Edge<K, EV>> records = collector.run(edges).execute(executionName);

		for (Edge<K, EV> result : records) {
			System.out.println(result);
		}

	}

	@Override
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		edges
			.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
				.name("CSV: " + filename);
	}
}
