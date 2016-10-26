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
public abstract class SimpleDriver<R extends PrintableResult>
extends ParameterizedBase {

	protected DataSet<? extends R> result;

	public void hash(String executionName) throws Exception {
		Checksum checksum = new ChecksumHashCode<R>()
			.run((DataSet<R>) result)
			.execute(executionName);

		System.out.println(checksum);
	}

	public void print(String executionName) throws Exception {
		Collect<R> collector = new Collect<>();

		// Refactored due to openjdk7 compile error: https://travis-ci.org/greghogan/flink/builds/200487761
		List<R> records = collector.run((DataSet<R>) result).execute(executionName);

		for (R result : records) {
			System.out.println(result.toPrintableString());
		}
	}

	public void writeCSV(String filename, String lineDelimiter, String fieldDelimiter) {
		result
			.writeAsCsv(filename, lineDelimiter, fieldDelimiter)
				.name("CSV: " + filename);
	}
}
