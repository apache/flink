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
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;

import java.util.List;

/**
 * A base driver storing a single result {@link DataSet} with values
 * implementing {@link PrintableResult}.
 *
 * @param <R> algorithm's result type
 */
public abstract class SimpleDriver<K, VV, EV, R extends PrintableResult>
extends ParameterizedBase
implements Driver<K, VV, EV> {

	private DataSet<R> result;

	protected DataSet<R> getResult() {
		return result;
	}

	/**
	 * Plan the algorithm and return the result {@link DataSet}.
	 *
	 * @param graph input graph
	 * @return driver output
	 * @throws Exception on error
	 */
	protected abstract DataSet<R> simplePlan(Graph<K, VV, EV> graph) throws Exception;

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		result = simplePlan(graph);
	}

	/**
	 * Print hash of execution results.
	 *
	 * Does *not* implement/override {@code Hash} since {@link Driver}
	 * implementations designate the appropriate outputs.
	 *
	 * @param executionName job name
	 * @throws Exception on error
	 */
	public void hash(String executionName) throws Exception {
		Checksum checksum = new ChecksumHashCode<R>()
			.run(result)
			.execute(executionName);

		System.out.println(checksum);
	}

	/**
	 * Print execution results.
	 *
	 * Does *not* implement/override {@code Print} since {@link Driver}
	 * implementations designate the appropriate outputs.
	 *
	 * @param executionName job name
	 * @throws Exception on error
	 */
	public void print(String executionName) throws Exception {
		List<R> results = new Collect<R>().run(result).execute(executionName);

		for (R result : results) {
			System.out.println(result.toPrintableString());
		}
	}

	/**
	 * Write execution results to file using CSV format.
	 *
	 * Does *not* implement/override {@code CSV} since {@link Driver}
	 * implementations designate the appropriate outputs.
	 *
	 * @param filename output filename
	 * @param lineDelimiter CSV delimiter between lines
	 * @param fieldDelimiter CSV delimiter between fields
	 */
	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		result
			.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
				.name("CSV: " + filename);
	}
}
