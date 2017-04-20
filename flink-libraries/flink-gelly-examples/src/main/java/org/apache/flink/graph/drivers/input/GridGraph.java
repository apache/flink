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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Generate a {@link org.apache.flink.graph.generator.GridGraph}.
 */
public class GridGraph
extends ParameterizedBase
implements Input<LongValue, NullValue, NullValue> {

	private static final String PREFIX = "dim";

	private List<Dimension> dimensions = new ArrayList<>();

	private LongParameter littleParallelism = new LongParameter(this, "little_parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT);

	@Override
	public String getName() {
		return GridGraph.class.getSimpleName();
	}

	@Override
	public String getUsage() {
		return "--dim0 size:wrap_endpoints [--dim1 size:wrap_endpoints [--dim2 ...]]" + super.getUsage();
	}

	@Override
	public void configure(ParameterTool parameterTool) throws ProgramParametrizationException {
		super.configure(parameterTool);

		// add dimensions as ordered by dimension ID (dim0, dim1, dim2, ...)

		Map<Integer, String> dimensionMap = new TreeMap<>();

		// first parse all dimensions into a sorted map
		for (Map.Entry<String, String> entry : parameterTool.toMap().entrySet()) {
			if (entry.getKey().startsWith(PREFIX)) {
				int dimensionId = Integer.parseInt(entry.getKey().substring(PREFIX.length()));
				dimensionMap.put(dimensionId, entry.getValue());
			}
		}

		// then store dimensions in order
		for (String field : dimensionMap.values()) {
			dimensions.add(new Dimension(field));
		}
	}

	@Override
	public String getIdentity() {
		return getName() + " (" + dimensions + ")";
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> create(ExecutionEnvironment env) {
		org.apache.flink.graph.generator.GridGraph graph = new org.apache.flink.graph.generator.GridGraph(env);

		for (Dimension dimension : dimensions) {
			graph.addDimension(dimension.size, dimension.wrapEndpoints);
		}

		return graph
			.setParallelism(littleParallelism.getValue().intValue())
			.generate();
	}

	/**
	 * Stores and parses the size and endpoint wrapping configuration for a
	 * {@link org.apache.flink.graph.generator.GridGraph} dimension.
	 */
	private static class Dimension {
		private long size;

		private boolean wrapEndpoints;

		/**
		 * Configuration string to be parsed. The size integer and endpoint
		 * wrapping boolean must be separated by a colon.
		 *
		 * @param field configuration string
		 */
		public Dimension(String field) {
			ProgramParametrizationException exception = new ProgramParametrizationException("Grid dimension must use " +
				"a colon to separate the integer size and boolean indicating whether the dimension endpoints are " +
				"connected: '" + field + "'");

			if (! field.contains(":")) {
				throw exception;
			}

			String[] parts = field.split(":");

			if (parts.length != 2) {
				throw exception;
			}

			try {
				size = Long.parseLong(parts[0]);
				wrapEndpoints = Boolean.parseBoolean(parts[1]);
			} catch(NumberFormatException ex) {
				throw exception;
			}
		}

		@Override
		public String toString() {
			return Long.toString(size) + (wrapEndpoints ? "+" : "⊞");
		}
	}
}
