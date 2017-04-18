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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.graph.library.GSAConnectedComponents;

import java.util.List;

/**
 * Driver for {@link org.apache.flink.graph.library.GSAConnectedComponents}.
 *
 * The gather-sum-apply implementation is used because scatter-gather does not
 * handle object reuse (see FLINK-5891).
 */
public class ConnectedComponents<K extends Comparable<K>, VV, EV>
extends ParameterizedBase
implements Driver<K, VV, EV>, CSV, Hash, Print {

	private DataSet<Vertex<K, K>> components;

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "ConnectedComponents";
	}

	@Override
	public String getLongDescription() {
		return "ConnectedComponents";
	}

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		components = graph
			.mapVertices(new MapVertices<K, VV>())
			.run(new GSAConnectedComponents<K, K, EV>(Integer.MAX_VALUE));
	}

	@Override
	public void hash(String executionName) throws Exception {
		Checksum checksum = new ChecksumHashCode<Vertex<K, K>>()
			.run(components)
			.execute(executionName);

		System.out.println(checksum);
	}

	@Override
	public void print(String executionName) throws Exception {
		Collect<Vertex<K, K>> collector = new Collect<>();

		// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
		List<Vertex<K, K>> records = collector.run(components).execute(executionName);

		for (Vertex<K, K> result : records) {
			System.out.println(result);
		}
	}

	@Override
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		components
			.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
				.name("CSV: " + filename);
	}

	private static final class MapVertices<T, VT>
	implements MapFunction<Vertex<T, VT>, T> {
		@Override
		public T map(Vertex<T, VT> value) throws Exception {
			return value.f0;
		}
	}
}
