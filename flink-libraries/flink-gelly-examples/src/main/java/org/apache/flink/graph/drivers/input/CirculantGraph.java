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

package org.apache.flink.graph.drivers.input;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.graph.generator.CirculantGraph.MINIMUM_VERTEX_COUNT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Generate a {@link org.apache.flink.graph.generator.CirculantGraph}.
 */
public class CirculantGraph
extends GeneratedGraph<LongValue> {

	private static final String PREFIX = "off";

	private LongParameter vertexCount = new LongParameter(this, "vertex_count")
			.setMinimumValue(MINIMUM_VERTEX_COUNT);

	private List<OffsetRange> offsetRanges = new ArrayList<>();

	private LongParameter littleParallelism = new LongParameter(this, "little_parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT);

	@Override
	public String getName() {
		return CirculantGraph.class.getSimpleName();
	}

	@Override
	public String getUsage() {
		return "--off0 offset:length [--off1 offset:length [--off2 ...]]" + super.getUsage();
	}

	@Override
	public void configure(ParameterTool parameterTool) throws ProgramParametrizationException {
		super.configure(parameterTool);

		// add offset ranges as ordered by offset ID (off0, off1, off2, ...)

		Map<Integer, String> offsetRangeMap = new TreeMap<>();

		// first parse all offset ranges into a sorted map
		for (String key : parameterTool.toMap().keySet()) {
			if (key.startsWith(PREFIX)) {
				int offsetId = Integer.parseInt(key.substring(PREFIX.length()));
				offsetRangeMap.put(offsetId, parameterTool.get(key));
			}
		}

		// then store offset ranges in order
		for (String field : offsetRangeMap.values()) {
			offsetRanges.add(new OffsetRange(field));
		}
	}

	@Override
	public String getIdentity() {
		return getTypeName() + " " + getName() + " (" + offsetRanges + ")";
	}

	@Override
	protected long vertexCount() {
		return vertexCount.getValue();
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate(ExecutionEnvironment env) {
		org.apache.flink.graph.generator.CirculantGraph graph = new org.apache.flink.graph.generator.CirculantGraph(env, vertexCount.getValue());

		for (OffsetRange offsetRange : offsetRanges) {
			graph.addOffsetRange(offsetRange.offset, offsetRange.length);
		}

		return graph
			.setParallelism(littleParallelism.getValue().intValue())
			.generate();
	}

	/**
	 * Stores and parses the start offset and length configuration for a
	 * {@link org.apache.flink.graph.generator.CirculantGraph} offset range.
	 */
	private static class OffsetRange {
		private long offset;

		private long length;

		/**
		 * Configuration string to be parsed. The offset integer and length integer
		 * length integer must be separated by a colon.
		 *
		 * @param field configuration string
		 */
		public OffsetRange(String field) {
			ProgramParametrizationException exception = new ProgramParametrizationException("Circulant offset range must use " +
				"a colon to separate the integer offset and integer length:" + field + "'");

			if (! field.contains(":")) {
				throw exception;
			}

			String[] parts = field.split(":");

			if (parts.length != 2) {
				throw exception;
			}

			try {
				offset = Long.parseLong(parts[0]);
				length = Long.parseLong(parts[1]);
			} catch(NumberFormatException ex) {
				throw exception;
			}
		}

		@Override
		public String toString() {
			return Long.toString(offset) + ":" + Long.toString(length);
		}
	}
}
