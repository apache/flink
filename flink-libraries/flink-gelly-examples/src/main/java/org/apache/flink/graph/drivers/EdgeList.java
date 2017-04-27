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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.graph.utils.EdgeToTuple2Map;
import org.apache.flink.types.NullValue;

import java.util.List;

/**
 * Convert a {@link Graph} to the {@link DataSet} of {@link Edge}.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
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
		List<Edge<K, EV>> records = new Collect<Edge<K, EV>>().run(edges).execute(executionName);

		if (hasNullValueEdges(edges)) {
			for (Edge<K, EV> result : records) {
				System.out.println("(" + result.f0 + "," + result.f1 + ")");
			}
		} else {
			for (Edge<K, EV> result : records) {
				System.out.println(result);
			}
		}
	}

	@Override
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		if (hasNullValueEdges(edges)) {
			edges
				.map(new EdgeToTuple2Map<K, EV>())
					.name("Edge to Tuple2")
				.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
					.name("CSV: " + filename);
		} else {
			edges
				.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
					.name("CSV: " + filename);
		}
	}

	/**
	 * Check whether the edge type of the {@link DataSet} is {@link NullValue}.
	 *
	 * @param edges data set for introspection
	 * @param <T> graph ID type
	 * @param <ET> edge value type
	 * @return whether the edge type of the {@link DataSet} is {@link NullValue}
	 */
	private static <T, ET> boolean hasNullValueEdges(DataSet<Edge<T, ET>> edges) {
		TypeInformation<?> genericTypeInfo = edges.getType();
		@SuppressWarnings("unchecked")
		TupleTypeInfo<Tuple3<T, T, ET>> tupleTypeInfo = (TupleTypeInfo<Tuple3<T, T, ET>>) genericTypeInfo;

		return tupleTypeInfo.getTypeAt(2).equals(ValueTypeInfo.NULL_VALUE_TYPE_INFO);
	}
}
