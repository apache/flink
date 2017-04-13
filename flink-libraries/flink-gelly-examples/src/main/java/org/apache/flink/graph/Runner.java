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

package org.apache.flink.graph;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.drivers.AdamicAdar;
import org.apache.flink.graph.drivers.ClusteringCoefficient;
import org.apache.flink.graph.drivers.ConnectedComponents;
import org.apache.flink.graph.drivers.Driver;
import org.apache.flink.graph.drivers.EdgeList;
import org.apache.flink.graph.drivers.GraphMetrics;
import org.apache.flink.graph.drivers.HITS;
import org.apache.flink.graph.drivers.JaccardIndex;
import org.apache.flink.graph.drivers.PageRank;
import org.apache.flink.graph.drivers.TriangleListing;
import org.apache.flink.graph.drivers.input.CompleteGraph;
import org.apache.flink.graph.drivers.input.CycleGraph;
import org.apache.flink.graph.drivers.input.EmptyGraph;
import org.apache.flink.graph.drivers.input.GridGraph;
import org.apache.flink.graph.drivers.input.HypercubeGraph;
import org.apache.flink.graph.drivers.input.Input;
import org.apache.flink.graph.drivers.input.PathGraph;
import org.apache.flink.graph.drivers.input.RMatGraph;
import org.apache.flink.graph.drivers.input.SingletonEdgeGraph;
import org.apache.flink.graph.drivers.input.StarGraph;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.Parameterized;
import org.apache.flink.util.InstantiationUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This default main class executes Flink drivers.
 *
 * An execution has one input, one algorithm, and one output. Anything more
 * complex can be expressed as a user program written in a JVM language.
 *
 * Inputs and algorithms are decoupled by, respectively, producing and
 * consuming a graph. Currently only {@code Graph} is supported but later
 * updates may add support for new graph types such as {@code BipartiteGraph}.
 *
 * Algorithms must explicitly support each type of output via implementation of
 * interfaces. This is scalable as the number of outputs is small and finite.
 */
public class Runner {

	private static final String INPUT = "input";

	private static final String ALGORITHM = "algorithm";

	private static final String OUTPUT = "output";

	private static ParameterizedFactory<Input> inputFactory = new ParameterizedFactory<Input>()
		.addClass(CompleteGraph.class)
		.addClass(org.apache.flink.graph.drivers.input.CSV.class)
		.addClass(CycleGraph.class)
		.addClass(EmptyGraph.class)
		.addClass(GridGraph.class)
		.addClass(HypercubeGraph.class)
		.addClass(PathGraph.class)
		.addClass(RMatGraph.class)
		.addClass(SingletonEdgeGraph.class)
		.addClass(StarGraph.class);

	private static ParameterizedFactory<Driver> driverFactory = new ParameterizedFactory<Driver>()
		.addClass(AdamicAdar.class)
		.addClass(ClusteringCoefficient.class)
		.addClass(ConnectedComponents.class)
		.addClass(EdgeList.class)
		.addClass(GraphMetrics.class)
		.addClass(HITS.class)
		.addClass(JaccardIndex.class)
		.addClass(PageRank.class)
		.addClass(TriangleListing.class);

	/**
	 * List available algorithms. This is displayed to the user when no valid
	 * algorithm is given in the program parameterization.
	 *
	 * @return usage string listing available algorithms
	 */
	private static String getAlgorithmsListing() {
		StrBuilder strBuilder = new StrBuilder();

		strBuilder
			.appendNewLine()
			.appendln("Select an algorithm to view usage: flink run examples/flink-gelly-examples_<version>.jar --algorithm <algorithm>")
			.appendNewLine()
			.appendln("Available algorithms:");

		for (Driver algorithm : driverFactory) {
			strBuilder.append("  ")
				.appendFixedWidthPadRight(algorithm.getName(), 30, ' ')
				.append(algorithm.getShortDescription()).appendNewLine();
		}

		return strBuilder.toString();
	}

	/**
	 * Display the usage for the given algorithm. This includes options for all
	 * compatible inputs, the selected algorithm, and outputs implemented by
	 * the selected algorithm.
	 *
	 * @param algorithmName unique identifier of the selected algorithm
	 * @return usage string for the given algorithm
	 */
	private static String getAlgorithmUsage(String algorithmName) {
		StrBuilder strBuilder = new StrBuilder();

		Driver algorithm = driverFactory.get(algorithmName);

		strBuilder
			.appendNewLine()
			.appendNewLine()
			.appendln(algorithm.getLongDescription())
			.appendNewLine()
			.append("usage: flink run examples/flink-gelly-examples_<version>.jar --algorithm ")
			.append(algorithmName)
			.append(" [algorithm options] --input <input> [input options] --output <output> [output options]")
			.appendNewLine()
			.appendNewLine()
			.appendln("Available inputs:");

		for (Input input : inputFactory) {
			strBuilder
				.append("  --input ")
				.append(input.getName())
				.append(" ")
				.appendln(input.getUsage());
		}

		String algorithmParameterization = algorithm.getUsage();

		if (algorithmParameterization.length() > 0) {
			strBuilder
				.appendNewLine()
				.appendln("Algorithm configuration:")
				.append("  ")
				.appendln(algorithm.getUsage());
		}

		strBuilder
			.appendNewLine()
			.appendln("Available outputs:");

		if (algorithm instanceof org.apache.flink.graph.drivers.output.CSV) {
			strBuilder.appendln("  --output csv --output_filename FILENAME [--output_line_delimiter LINE_DELIMITER] [--output_field_delimiter FIELD_DELIMITER]");
		}

		if (algorithm instanceof Hash) {
			strBuilder.appendln("  --output hash");
		}

		if (algorithm instanceof Print) {
			strBuilder.appendln("  --output print");
		}

		return strBuilder
			.appendNewLine()
			.toString();
	}

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ExecutionConfig config = env.getConfig();

		// should not have any non-Flink data types
		config.disableAutoTypeRegistration();
		config.disableForceAvro();
		config.disableForceKryo();

		ParameterTool parameters = ParameterTool.fromArgs(args);
		config.setGlobalJobParameters(parameters);

		// integration tests run with with object reuse both disabled and enabled
		if (parameters.has("__disable_object_reuse")) {
			config.disableObjectReuse();
		} else {
			config.enableObjectReuse();
		}

		// Usage

		if (!parameters.has(ALGORITHM)) {
			throw new ProgramParametrizationException(getAlgorithmsListing());
		}

		String algorithmName = parameters.get(ALGORITHM);
		Driver algorithm = driverFactory.get(algorithmName);

		if (algorithm == null) {
			throw new ProgramParametrizationException("Unknown algorithm name: " + algorithmName);
		}

		if (!parameters.has(INPUT)) {
			if (!parameters.has(OUTPUT)) {
				// if neither input nor output is given then print algorithm usage
				throw new ProgramParametrizationException(getAlgorithmUsage(algorithmName));
			}
			throw new ProgramParametrizationException("No input given");
		}

		String inputName = parameters.get(INPUT);
		Input input = inputFactory.get(inputName);

		if (input == null) {
			throw new ProgramParametrizationException("Unknown input type: " + inputName);
		}

		// Input

		input.configure(parameters);
		Graph graph = input.create(env);

		// Algorithm

		algorithm.configure(parameters);
		algorithm.plan(graph);

		// Output
		if (!parameters.has(OUTPUT)) {
			throw new ProgramParametrizationException("No output given");
		}

		String outputName = parameters.get(OUTPUT);
		String executionNamePrefix = input.getIdentity() + " -> " + algorithmName + " -> ";

		System.out.println();

		switch (outputName.toLowerCase()) {
			case "csv":
				if (algorithm instanceof org.apache.flink.graph.drivers.output.CSV) {
					String filename = parameters.getRequired("output_filename");

					String lineDelimiter = StringEscapeUtils.unescapeJava(
						parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

					String fieldDelimiter = StringEscapeUtils.unescapeJava(
						parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

					org.apache.flink.graph.drivers.output.CSV c = (org.apache.flink.graph.drivers.output.CSV) algorithm;
					c.writeCSV(filename, lineDelimiter, fieldDelimiter);

					env.execute(executionNamePrefix + "CSV");
				} else {
					throw new ProgramParametrizationException("Algorithm does not support output type 'CSV'");
				}
				break;

			case "hash":
				if (algorithm instanceof Hash) {
					Hash h = (Hash) algorithm;
					h.hash(executionNamePrefix + "Hash");
				} else {
					throw new ProgramParametrizationException("Algorithm does not support output type 'hash'");
				}
				break;

			case "print":
				if (algorithm instanceof Print) {
					Print h = (Print) algorithm;
					h.print(executionNamePrefix + "Print");
				} else {
					throw new ProgramParametrizationException("Algorithm does not support output type 'print'");
				}
				break;

			default:
				throw new ProgramParametrizationException("Unknown output type: " + outputName);
		}
	}

	/**
	 * Stores a list of classes for which an instance can be requested by name
	 * and implements an iterator over class instances.
	 *
	 * @param <T> base type for stored classes
	 */
	private static class ParameterizedFactory<T extends Parameterized>
	implements Iterable<T> {
		private List<Class<? extends T>> classes = new ArrayList<>();

		/**
		 * Add a class to the factory.
		 *
		 * @param cls subclass of T
		 * @return this
		 */
		public ParameterizedFactory<T> addClass(Class<? extends T> cls) {
			this.classes.add(cls);
			return this;
		}

		/**
		 * Obtain a class instance by name.
		 *
		 * @param name String matching getName()
		 * @return class instance or null if no matching class
		 */
		public T get(String name) {
			for (T instance : this) {
				if (name.equals(instance.getName())) {
					return instance;
				}
			}

			return null;
		}

		@Override
		public Iterator<T> iterator() {
			return new Iterator<T>() {
				private int index;

				@Override
				public boolean hasNext() {
					return index < classes.size();
				}

				@Override
				public T next() {
					return InstantiationUtil.instantiate(classes.get(index++));
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}
}
