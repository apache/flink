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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.utils.Tuple2ToEdgeMap;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

/**
 * A class to build a Graph using path(s) provided to CSV file(s) with optional vertex and edge data.
 * The class also configures the CSV readers used to read edge and vertex data such as the field types,
 * the delimiters (row and field), the fields that should be included or skipped, and other flags,
 * such as whether to skip the initial line as the header.
 * The configuration is done using the functions provided in the {@link org.apache.flink.api.java.io.CsvReader} class.
 */

public class GraphCsvReader {

	@SuppressWarnings("unused")
	private final Path vertexPath, edgePath;
	private final ExecutionEnvironment executionContext;
	protected CsvReader edgeReader;
	protected CsvReader vertexReader;
	protected MapFunction<?, ?> mapper;

//--------------------------------------------------------------------------------------------------------------------
	public GraphCsvReader(Path vertexPath, Path edgePath, ExecutionEnvironment context) {
		this.vertexPath = vertexPath;
		this.edgePath = edgePath;
		this.vertexReader = new CsvReader(vertexPath, context);
		this.edgeReader = new CsvReader(edgePath, context);
		this.mapper = null;
		this.executionContext = context;
	}

	public GraphCsvReader(Path edgePath, ExecutionEnvironment context) {
		this.vertexPath = null;
		this.edgePath = edgePath;
		this.edgeReader = new CsvReader(edgePath, context);
		this.vertexReader = null;
		this.mapper = null;
		this.executionContext = context;
	}

	public <K, VV> GraphCsvReader(Path edgePath, final MapFunction<K, VV> mapper, ExecutionEnvironment context) {
		this.vertexPath = null;
		this.edgePath = edgePath;
		this.edgeReader = new CsvReader(edgePath, context);
		this.vertexReader = null;
		this.mapper = mapper;
		this.executionContext = context;
	}

	public GraphCsvReader (String edgePath, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);

	}

	public GraphCsvReader(String vertexPath, String edgePath, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(vertexPath, "The file path may not be null.")),
				new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);
	}

	public <K, VV> GraphCsvReader(String edgePath, final MapFunction<K, VV> mapper, ExecutionEnvironment context) {
			this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), mapper, context);
	}

	/**
	 * Creates a Graph from CSV input with vertex values and edge values.
	 * The vertex values are specified through a vertices input file or a user-defined map function.
	 *
	 * @param vertexKey the type of the vertex IDs
	 * @param vertexValue the type of the vertex values
	 * @param edgeValue the type of the edge values
	 * @return a Graph with vertex and edge values.
	 */
	@SuppressWarnings("unchecked")
	public <K, VV, EV> Graph<K, VV, EV> types(Class<K> vertexKey, Class<VV> vertexValue,
			Class<EV> edgeValue) {

		if (edgeReader == null) {
			throw new RuntimeException("The edge input file cannot be null!");
		}

		DataSet<Tuple3<K, K, EV>> edges = edgeReader.types(vertexKey, vertexKey, edgeValue);

		// the vertex value can be provided by an input file or a user-defined mapper
		if (vertexReader != null) {
			DataSet<Tuple2<K, VV>> vertices = vertexReader
				.types(vertexKey, vertexValue)
					.name(GraphCsvReader.class.getName());

			return Graph.fromTupleDataSet(vertices, edges, executionContext);
		}
		else if (mapper != null) {
			return Graph.fromTupleDataSet(edges, (MapFunction<K, VV>) mapper, executionContext);
		}
		else {
			throw new RuntimeException("Vertex values have to be specified through a vertices input file"
					+ "or a user-defined map function.");
		}
	}

	/**
	 * Creates a Graph from CSV input with edge values, but without vertex values.
	 * @param vertexKey the type of the vertex IDs
	 * @param edgeValue the type of the edge values
	 * @return a Graph where the edges are read from an edges CSV file (with values).
	 */
	public <K, EV> Graph<K, NullValue, EV> edgeTypes(Class<K> vertexKey, Class<EV> edgeValue) {

		if (edgeReader == null) {
			throw new RuntimeException("The edge input file cannot be null!");
		}

		DataSet<Tuple3<K, K, EV>> edges = edgeReader
			.types(vertexKey, vertexKey, edgeValue)
				.name(GraphCsvReader.class.getName());

		return Graph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 * Creates a Graph from CSV input without vertex values or edge values.
	 * @param vertexKey the type of the vertex IDs
	 * @return a Graph where the vertex IDs are read from the edges input file.
	 */
	public <K> Graph<K, NullValue, NullValue> keyType(Class<K> vertexKey) {

		if (edgeReader == null) {
			throw new RuntimeException("The edge input file cannot be null!");
		}

		DataSet<Edge<K, NullValue>> edges = edgeReader
			.types(vertexKey, vertexKey)
				.name(GraphCsvReader.class.getName())
			.map(new Tuple2ToEdgeMap<>())
				.name("Type conversion");

		return Graph.fromDataSet(edges, executionContext);
	}

	/**
	 * Creates a Graph from CSV input without edge values.
	 * The vertex values are specified through a vertices input file or a user-defined map function.
	 * If no vertices input file is provided, the vertex IDs are automatically created from the edges
	 * input file.
	 * @param vertexKey the type of the vertex IDs
	 * @param vertexValue the type of the vertex values
	 * @return a Graph where the vertex IDs and vertex values.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public <K, VV> Graph<K, VV, NullValue> vertexTypes(Class<K> vertexKey, Class<VV> vertexValue) {

		if (edgeReader == null) {
			throw new RuntimeException("The edge input file cannot be null!");
		}

		DataSet<Edge<K, NullValue>> edges = edgeReader
			.types(vertexKey, vertexKey)
				.name(GraphCsvReader.class.getName())
			.map(new Tuple2ToEdgeMap<>())
				.name("To Edge");

		// the vertex value can be provided by an input file or a user-defined mapper
		if (vertexReader != null) {
			DataSet<Vertex<K, VV>> vertices = vertexReader
				.types(vertexKey, vertexValue)
					.name(GraphCsvReader.class.getName())
				.map(new Tuple2ToVertexMap<>())
					.name("Type conversion");

			return Graph.fromDataSet(vertices, edges, executionContext);
		}
		else if (mapper != null) {
			return Graph.fromDataSet(edges, (MapFunction<K, VV>) mapper, executionContext);
		}
		else {
			throw new RuntimeException("Vertex values have to be specified through a vertices input file"
					+ "or a user-defined map function.");
		}
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the edges
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader lineDelimiterEdges(String delimiter) {
		edgeReader.lineDelimiter(delimiter);
		return this;
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the vertices
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader lineDelimiterVertices(String delimiter) {
		if (this.vertexReader != null) {
			this.vertexReader.lineDelimiter(delimiter);
		}
		return this;
	}

	/**
	 *Configures the Delimiter that separates fields in a row for the CSV reader used to read the vertices
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader fieldDelimiterVertices(String delimiter) {
		if (this.vertexReader != null) {
			this.vertexReader.fieldDelimiter(delimiter);
		}
		return this;
	}

	/**
	 *Configures the Delimiter that separates fields in a row for the CSV reader used to read the edges
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader fieldDelimiterEdges(String delimiter) {
		this.edgeReader.fieldDelimiter(delimiter);
		return this;
	}

	/**
	 * Enables quoted String parsing for Edge Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader parseQuotedStringsEdges(char quoteCharacter) {
		this.edgeReader.parseQuotedStrings(quoteCharacter);
		return this;
	}

	/**
	 * Enables quoted String parsing for Vertex Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader parseQuotedStringsVertices(char quoteCharacter) {
		if (this.vertexReader != null) {
			this.vertexReader.parseQuotedStrings(quoteCharacter);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the Vertex Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreCommentsVertices(String commentPrefix) {
		if (this.vertexReader != null) {
			this.vertexReader.ignoreComments(commentPrefix);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the Edge Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreCommentsEdges(String commentPrefix) {
		this.edgeReader.ignoreComments(commentPrefix);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing vertices data should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 *
	 * @param vertexFields The array of flags that describes which fields are to be included from the CSV file for vertices.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(boolean ... vertexFields) {
		if (this.vertexReader != null) {
			this.vertexReader.includeFields(vertexFields);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing edges data should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 *
	 * @param edgeFields The array of flags that describes which fields are to be included from the CSV file for edges.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(boolean ... edgeFields) {
		this.edgeReader.includeFields(edgeFields);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing vertices data should be included and which should be skipped. The
	 * positions in the string (read from position 0 to its length) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the mask string
	 * The parser will skip over all fields where the character at the corresponding position
	 * in the string is {@code '0'}, {@code 'F'}, or {@code 'f'} (representing the value
	 * {@code false}). The result contains the fields where the corresponding position in
	 * the boolean array is {@code '1'}, {@code 'T'}, or {@code 't'} (representing the value {@code true}).
	 *
	 * @param mask The string mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(String mask) {
		if (this.vertexReader != null) {
			this.vertexReader.includeFields(mask);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing edges data should be included and which should be skipped. The
	 * positions in the string (read from position 0 to its length) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the mask string
	 * The parser will skip over all fields where the character at the corresponding position
	 * in the string is {@code '0'}, {@code 'F'}, or {@code 'f'} (representing the value
	 * {@code false}). The result contains the fields where the corresponding position in
	 * the boolean array is {@code '1'}, {@code 'T'}, or {@code 't'} (representing the value {@code true}).
	 *
	 * @param mask The string mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(String mask) {
		this.edgeReader.includeFields(mask);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing vertices data should be included and which should be skipped. The
	 * bits in the value (read from least significant to most significant) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the position of the most significant
	 * non-zero bit.
	 * The parser will skip over all fields where the character at the corresponding bit is zero, and
	 * include the fields where the corresponding bit is one.
	 *
	 * <p>Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(long mask) {
		if (this.vertexReader != null) {
			this.vertexReader.includeFields(mask);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing edges data should be included and which should be skipped. The
	 * bits in the value (read from least significant to most significant) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the position of the most significant
	 * non-zero bit.
	 * The parser will skip over all fields where the character at the corresponding bit is zero, and
	 * include the fields where the corresponding bit is one.
	 *
	 * <p>Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(long mask) {
		this.edgeReader.includeFields(mask);
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreFirstLineEdges() {
		this.edgeReader.ignoreFirstLine();
		return this;
	}

	/**
	 * Sets the CSV reader for the Vertices file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreFirstLineVertices() {
		if (this.vertexReader != null) {
			this.vertexReader.ignoreFirstLine();
		}
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreInvalidLinesEdges() {
		this.edgeReader.ignoreInvalidLines();
		return this;
	}

	/**
	 * Sets the CSV reader Vertices file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreInvalidLinesVertices() {
		if (this.vertexReader != null) {
			this.vertexReader.ignoreInvalidLines();
		}
		return this;
	}
}
