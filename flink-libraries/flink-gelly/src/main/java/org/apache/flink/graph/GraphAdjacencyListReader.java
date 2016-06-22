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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;

/**
 * A class to build a Graph using path(s) provided to a text file with adjacency list data.
 * The class also configures the vertex id as well as the vertex and edge value types and the delimiters
 * separating vertices ids and vertices/edge values.
 */

public class GraphAdjacencyListReader {

	private final String fileName;
	private final ExecutionEnvironment executionContext;
	protected String SOURCE_NEIGHBOR_DELIMITER = "\\s+";
	protected String VERTEX_VALUE_DELIMITER = "-";
	protected String VERTICES_DELIMITER = ",";

	public GraphAdjacencyListReader(String fileName, ExecutionEnvironment context) {
		this.fileName = Preconditions.checkNotNull(fileName, "The file name may not be null.");
		this.executionContext = context;
	}

	public GraphAdjacencyListReader(Path filePath, ExecutionEnvironment context) {
		this(filePath.toString(), context);
	}

	/**
	 * Configures the Delimiter that separates the source vertex id and value from its neighbors.
	 * ({@code '\t'}) is used by default
	 *
	 * @param delimiter The delimiter that separates the source node its neighbors.
	 * @return The GraphAdjacencyListReader instance itself, to allow for fluent function chaining.
	 */
	public GraphAdjacencyListReader vertexNeighborsDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}
		this.SOURCE_NEIGHBOR_DELIMITER = checkDelimiterSpecialChar(delimiter);
		return this;
	}

	/**
	 * Configures the Delimiter that separates the vertices ids from their values as well as the
	 * edges destinations ids from the edge value
	 * ({@code '-'}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the vertices ids from the vertices
	 *                  and edge values.
	 * @return The GraphAdjacencyListReader instance itself, to allow for fluent function chaining.
	 */
	public GraphAdjacencyListReader vertexValueDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}
		this.VERTEX_VALUE_DELIMITER = checkDelimiterSpecialChar(delimiter);
		return this;
	}

	/**
	 * Configures the Delimiter that separates the neighboring vertices of a vertex
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the neighboring vertices of a vertex.
	 * @return The GraphAdjacencyListReader instance itself, to allow for fluent function chaining.
	 */
	public GraphAdjacencyListReader verticesDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}
		this.VERTICES_DELIMITER = checkDelimiterSpecialChar(delimiter);
		return this;
	}

	/**
	 * Creates a Graph from an AdjacencyList formatted text file without vertex or edge values.
	 *
	 * @param vertexKey the type of the vertex IDs
	 * @return a Graph where the vertex IDs are read from the AdjacencyList formatted text input file.
	 */
	public <K> Graph<K, NullValue, NullValue> keyType(Class<K> vertexKey) {

		final Class<K> vertexKeyClass = vertexKey;

		final String source_neigh_del = SOURCE_NEIGHBOR_DELIMITER;
		final String vertices_del = VERTICES_DELIMITER;

		DataSet<String> edgesString = this.executionContext.readTextFile(this.fileName);

		DataSet<Tuple3<K, K, NullValue>> edges = edgesString.flatMap(new FlatMapFunction<String, Tuple3<K, K,
				NullValue>>() {
			@Override
			public void flatMap(String adjacencyLine, Collector<Tuple3<K, K, NullValue>> out) throws Exception {

				String[] sourceNeighbors = adjacencyLine.split(source_neigh_del);

				if (sourceNeighbors.length > 1) {
					String[] neighbors = sourceNeighbors[1].split(vertices_del);

					for (int i = 0; i < neighbors.length; i++) {
						Tuple3<K, K, NullValue> temp = new Tuple3<K, K, NullValue>(fromString(sourceNeighbors[0].trim
								(), vertexKeyClass),
								fromString(neighbors[i].trim(), vertexKeyClass), NullValue.getInstance());
						out.collect(temp);
					}
				}
			}
		}).returns(new TupleTypeInfo<Tuple3<K, K, NullValue>>(TypeExtractor.getForClass(vertexKeyClass),
				TypeExtractor.getForClass(vertexKeyClass), TypeExtractor.getForClass(NullValue.class)));

		return Graph.fromTupleDataSet(edges, executionContext);
	}


	/**
	 * Creates a Graph from an AdjacencyList formatted text file without edge values.
	 * The vertex values are provided in the input file or a user-defined map function.
	 * If no vertices input file is provided, the vertex IDs are automatically created from the edges
	 * input file.
	 *
	 * @param vertexKey   the type of the vertex IDs
	 * @param vertexValue the type of the vertex values
	 * @return a Graph with vertex values.
	 */
	@SuppressWarnings({"serial", "unchecked"})
	public <K, VV> Graph<K, VV, NullValue> vertexTypes(Class<K> vertexKey, Class<VV> vertexValue) {

		final Class<K> vertexKeyClass = vertexKey;
		final Class<VV> vertexValueClass = vertexValue;

		final String source_neigh_del = SOURCE_NEIGHBOR_DELIMITER;
		final String vertex_value_del = VERTEX_VALUE_DELIMITER;
		final String vertices_del = VERTICES_DELIMITER;

		DataSet<String> adjacencyListSource = this.executionContext.readTextFile(this.fileName);

		DataSet<Tuple2<K, VV>> vertices = adjacencyListSource.flatMap(new FlatMapFunction<String, Tuple2<K, VV>>() {
			@Override
			public void flatMap(String adjacencyLine, Collector<Tuple2<K, VV>> out) throws Exception {
				String[] vertexNeighbors = adjacencyLine.split(source_neigh_del);
				String[] sourceNValue = vertexNeighbors[0].split(vertex_value_del);
				out.collect(new Tuple2(fromString(sourceNValue[0].trim(), vertexKeyClass), fromString(sourceNValue[1]
						.trim(), vertexValueClass)));
			}
		}).returns(new TupleTypeInfo<Tuple2<K, VV>>(TypeExtractor.getForClass(vertexKeyClass),
				TypeExtractor.getForClass(vertexValueClass)));


		DataSet<Tuple3<K, K, NullValue>> edges = adjacencyListSource.flatMap(new FlatMapFunction<String, Tuple3<K, K,
				NullValue>>() {
			@Override
			public void flatMap(String adjacencyLine, Collector<Tuple3<K, K, NullValue>> out) throws Exception {

				String[] sourceNeighbors = adjacencyLine.split(source_neigh_del);
				String[] sourceNValue = sourceNeighbors[0].split(vertex_value_del);

				if (sourceNeighbors.length > 1) {
					String[] neighbors = sourceNeighbors[1].split(vertices_del);
					for (int i = 0; i < neighbors.length; i++) {
						out.collect(new Tuple3<K, K, NullValue>(fromString(sourceNValue[0].trim(), vertexKeyClass),
								fromString(neighbors[0].trim(), vertexKeyClass), NullValue.getInstance()));
					}
				}
			}
		}).returns(new TupleTypeInfo<Tuple3<K, K, NullValue>>(TypeExtractor.getForClass(vertexKeyClass),
				TypeExtractor.getForClass(vertexKeyClass), TypeExtractor.getForClass(NullValue.class)));

		return Graph.fromTupleDataSet(vertices, edges, executionContext);
	}

	/**
	 * Creates a Graph from an AdjacencyList formatted text file with edge values, but without vertex values.
	 *
	 * @param vertexKey the type of the vertex IDs
	 * @param edgeValue the type of the edge values
	 * @return a Graph where with edge values.
	 */
	public <K, EV> Graph<K, NullValue, EV> edgeTypes(Class<K> vertexKey, Class<EV> edgeValue) {

		final Class<K> vertexKeyClass = vertexKey;
		final Class<EV> edgeValueClass = edgeValue;

		final String source_neigh_del = SOURCE_NEIGHBOR_DELIMITER;
		final String vertex_value_del = VERTEX_VALUE_DELIMITER;
		final String vertices_del = VERTICES_DELIMITER;

		DataSet<Tuple3<K, K, EV>> edges = this.executionContext.readTextFile(this.fileName)
				.flatMap(new FlatMapFunction<String, Tuple3<K, K, EV>>() {
					@Override
					public void flatMap(String adjacencyLine, Collector<Tuple3<K, K, EV>> out) throws Exception {

						String[] sourceNeighbors = adjacencyLine.split(source_neigh_del);

						if (sourceNeighbors.length > 1) {
							String[] neighbors = sourceNeighbors[1].split(vertices_del);
							for (int i = 0; i < neighbors.length; i++) {
								String[] neighborNValue = neighbors[i].split(vertex_value_del);
								out.collect(new Tuple3<K, K, EV>(fromString(sourceNeighbors[0].trim(), vertexKeyClass),
										fromString(neighborNValue[0].trim(), vertexKeyClass), fromString
										(neighborNValue[1].trim(), edgeValueClass)));
							}
						}
					}
				}).returns(new TupleTypeInfo<Tuple3<K, K, EV>>(TypeExtractor.getForClass(vertexKeyClass),
						TypeExtractor.getForClass(vertexKeyClass), TypeExtractor.getForClass(edgeValue)));

		return Graph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 * Creates a Graph from an AdjacencyList formatted text file with vertex values and edge values.
	 * The vertex values are specified through a vertices input file or a user-defined map function.
	 * If no vertices input file is provided, the vertex IDs are automatically created from the edges
	 * input file.
	 *
	 * @param vertexKey   the type of the vertex IDs
	 * @param vertexValue the type of the vertex values
	 * @param edgeValue   the type of the edge values
	 * @return a Graph with vertex and edge values.
	 */
	@SuppressWarnings("unchecked")
	public <K, VV, EV> Graph<K, VV, EV> types(Class<K> vertexKey, Class<VV> vertexValue, Class<EV> edgeValue) {

		final Class<K> vertexKeyClass = vertexKey;
		final Class<VV> vertexValueClass = vertexValue;
		final Class<EV> edgeValueClass = edgeValue;

		final String source_neigh_del = SOURCE_NEIGHBOR_DELIMITER;
		final String vertex_value_del = VERTEX_VALUE_DELIMITER;
		final String vertices_del = VERTICES_DELIMITER;

		DataSet<String> adjacencyListSource = this.executionContext.readTextFile(this.fileName);

		DataSet<Tuple2<K, VV>> vertices = adjacencyListSource.flatMap(new FlatMapFunction<String, Tuple2<K, VV>>() {
			@Override
			public void flatMap(String adjacencyLine, Collector<Tuple2<K, VV>> out) throws Exception {
				String[] vertexNeighbors = adjacencyLine.split(source_neigh_del);

				String[] sourceNValue = vertexNeighbors[0].split(vertex_value_del);

				out.collect(new Tuple2<K, VV>(fromString(sourceNValue[0].trim(), vertexKeyClass),
						fromString(sourceNValue[1].trim(), vertexValueClass)));
			}
		}).returns(new TupleTypeInfo<Tuple2<K, VV>>(TypeExtractor.getForClass(vertexKeyClass),
				TypeExtractor.getForClass(vertexValueClass)));


		DataSet<Tuple3<K, K, EV>> edges = adjacencyListSource.flatMap(new FlatMapFunction<String, Tuple3<K, K, EV>>() {
			@Override
			public void flatMap(String adjacencyLine, Collector<Tuple3<K, K, EV>> out) throws Exception {

				String[] sourceNeighbors = adjacencyLine.split(source_neigh_del);
				String[] sourceNValue = sourceNeighbors[0].split(vertex_value_del);

				if (sourceNeighbors.length > 1) {
					String[] neighbors = sourceNeighbors[1].split(vertices_del);
					for (int i = 0; i < neighbors.length; i++) {
						String[] neighborNValue = neighbors[i].split(vertex_value_del);

						out.collect(new Tuple3<K, K, EV>(fromString(sourceNValue[0].trim(), vertexKeyClass),
								fromString(neighborNValue[0].trim(), vertexKeyClass), fromString(neighborNValue[1]
								.trim(), edgeValueClass)));
					}
				}
			}
		}).returns(new TupleTypeInfo<Tuple3<K, K, EV>>(TypeExtractor.getForClass(vertexKeyClass),
				TypeExtractor.getForClass(vertexKeyClass), TypeExtractor.getForClass(edgeValue)));

		return Graph.fromTupleDataSet(vertices, edges, executionContext);
	}

	private static <T> T fromString(String input, Class<T> type) {

		if (type.isAssignableFrom(String.class)) {
			return type.cast(input);
		} else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
			return type.cast(Boolean.valueOf(input));
		} else if (type.equals(Integer.class) || type.equals(int.class)) {
			return type.cast(Integer.valueOf(input));
		} else if (type.equals(Double.class) || type.equals(double.class)) {
			return type.cast(Double.valueOf(input));
		} else if (type.equals(Float.class) || type.equals(float.class)) {
			return type.cast(Float.valueOf(input));
		} else if (type.equals(BigDecimal.class)) {
			return type.cast(new BigDecimal(input));
		} else if (type.equals(Long.class) || type.equals(long.class)) {
			return type.cast(Long.valueOf(input));
		} else if (type.equals(char.class)) {
			return type.cast(input); //check the casting
		}
		return null;
	}

	private String checkDelimiterSpecialChar(String delimiter) {
		if (delimiter.equals(".")) {
			return "\\.";
		} else if (delimiter.equals("|")) {
			return "\\|";
		} else {
			return delimiter;
		}
	}
}
