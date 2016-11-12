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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.utils.Tuple2ToTuple3Map;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

/**
 * A class to build a BipartiteGraph using path(s) provided to CSV file(s) with optional vertex and edge data.
 * The class also configures the CSV readers used to read edge and vertex data such as the field types,
 * the delimiters (row and field), the fields that should be included or skipped, and other flags,
 * such as whether to skip the initial line as the header.
 * The configuration is done using the functions provided in the {@link org.apache.flink.api.java.io.CsvReader} class.
 */
public class BipartiteGraphCsvReader {
	// CSV files paths
	private final Path topVertexPath;
	private final Path bottomVertexPath;
	private final Path edgePath;

	// Readers
	protected CsvReader edgeReader;
	protected CsvReader topVertexReader;
	protected CsvReader bottomVertexReader;

	private final ExecutionEnvironment executionContext;
	protected MapFunction<?, ?> topMapper;
	protected MapFunction<?, ?> bottomMapper;

	// Bipartite types
	protected Class<?> topVertexKey;
	protected Class<?> bottomVertexKey;
	protected Class<?> topVertexValue;
	protected Class<?> bottomVertexValue;
	protected Class<?> edgeValue;

	//--------------------------------------------------------------------------------------------------------------------

	/**
	 * Create BipartiteGraphCsvReader with paths to to top vertices, bottom vertices, and edges
	 * CSV files.
	 *
	 * @param topVertexPath path to the top vertices CSV file.
	 * @param bottomVertexPath path to the bottom vertices CSV file.
	 * @param edgePath path to the edges CSV file.
	 * @param context the Flink execution environment.
	 */
	public BipartiteGraphCsvReader(Path topVertexPath, Path bottomVertexPath, Path edgePath,
						ExecutionEnvironment context) {
		this.topVertexPath = topVertexPath;
		this.bottomVertexPath = bottomVertexPath;
		this.edgePath = edgePath;
		this.topVertexReader = new CsvReader(topVertexPath, context);
		this.bottomVertexReader = new CsvReader(bottomVertexPath, context);
		this.edgeReader = new CsvReader(edgePath, context);
		this.topMapper = null;
		this.bottomMapper = null;
		this.executionContext = context;
	}

	/**
	 * Create BipartiteGraphCsvReader with the path the edges CSV file. Vertices IDs will be deducted
	 * from the edges IDs.
	 *
	 * @param edgePath path to the edges CSV file.
	 * @param context the Flink execution environment.
	 */
	public BipartiteGraphCsvReader(Path edgePath, ExecutionEnvironment context) {
		this.topVertexPath = null;
		this.bottomVertexPath = null;
		this.edgePath = edgePath;
		this.edgeReader = new CsvReader(edgePath, context);
		this.topVertexReader = null;
		this.bottomVertexReader = null;
		this.topMapper = null;
		this.bottomMapper = null;
		this.executionContext = context;
	}

	/**
	 * Create BipartiteGraphCsvReader with the path to edges * CSV file and mappers to initialize vertices values
	 * from their IDs.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 *
	 * @param edgePath path to the edges CSV file.
	 * @param topMapper function to initialize top vertices values.
	 * @param bottomMapper function to initialize bottom vertices values.
	 * @param context the Flink execution environment.
	 */
	public <KT, KB, VVT, VVB> BipartiteGraphCsvReader(Path edgePath, MapFunction<KT, VVT> topMapper, MapFunction<KB, VVB> bottomMapper, ExecutionEnvironment context) {
		this.topVertexPath = null;
		this.bottomVertexPath = null;
		this.edgePath = edgePath;
		this.edgeReader = new CsvReader(edgePath, context);
		this.topVertexReader = null;
		this.bottomVertexReader = null;
		this.topMapper = topMapper;
		this.bottomMapper = bottomMapper;
		this.executionContext = context;
	}

	/**
	 * Create BipartiteGraphCsvReader with the path the edges CSV file. Vertices IDs will be deducted
	 * from the edges IDs.
	 *
	 * @param edgePath path to the edges CSV file.
	 * @param context the Flink execution environment.
	 */
	public BipartiteGraphCsvReader(String edgePath, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);
	}

	/**
	 * Create BipartiteGraphCsvReader with paths to to top vertices, bottom vertices, and edges
	 * CSV files.
	 *
	 * @param topVertexPath path to the top vertices CSV file.
	 * @param bottomVertexPath path to the bottom vertices CSV file.
	 * @param edgePath path to the edges CSV file.
	 * @param context the Flink execution environment.
	 */
	public BipartiteGraphCsvReader(String topVertexPath, String bottomVertexPath, String edgePath, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(topVertexPath, "The file path may not be null.")),
			new Path(Preconditions.checkNotNull(bottomVertexPath, "The file path may not be null.")),
			new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);
	}


	/**
	 * Create BipartiteGraphCsvReader with the path to edges * CSV file and mappers to initialize vertices values
	 * from their IDs.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 *
	 * @param edgePath path to the edges CSV file.
	 * @param topMapper function to initialize top vertices values.
	 * @param bottomMapper function to initialize bottom vertices values.
	 * @param context the Flink execution environment.
	 */
	public <KT, KB, VVT, VVB> BipartiteGraphCsvReader(String edgePath, MapFunction<KT, VVT> topMapper, MapFunction<KB, VVB> bottomMapper, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), topMapper, bottomMapper, context);
	}

	/**
	 * Creates a BipartiteGraph from CSV input with vertex values and edge values.
	 * The vertex values are specified through a vertices input file or a user-defined map function.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 * @param <EV> the edge value type
	 *
	 * @param topVertexKey the type of the top vertex IDs
	 * @param bottomVertexKey the type of the bottom vertex IDs
	 * @param topVertexValue the type of the top vertex values
	 * @param bottomVertexValue the type of the bottom vertex values
	 * @param edgeValue the type of the edge values
	 * @return a BipartiteGraph with vertex and edge values.
	 */
	@SuppressWarnings("unchecked")
	public <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> types(
				Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<VVT> topVertexValue,
				Class<VVB> bottomVertexValue, Class<EV> edgeValue) {
		if (edgeReader == null) {
			throw new RuntimeException("The edges input file cannot be null!");
		}

		DataSet<Tuple3<KT, KB, EV>> edges = edgeReader.types(topVertexKey, bottomVertexKey, edgeValue);

		// the vertex value can be provided by an input file or a user-defined mapper
		if (topVertexReader != null && bottomVertexReader != null) {
			DataSet<Tuple2<KT, VVT>> topVertices = topVertexReader.types(topVertexKey, topVertexValue);
			DataSet<Tuple2<KB, VVB>> bottomVertices = bottomVertexReader.types(bottomVertexKey, bottomVertexValue);

			return BipartiteGraph.fromTupleDataSet(topVertices, bottomVertices, edges, executionContext);
		}
		else if (topMapper != null && bottomMapper != null) {
			return BipartiteGraph.fromTupleDataSet(edges, (MapFunction<KT, VVT>) topMapper, (MapFunction<KB, VVB>) bottomMapper, executionContext);
		}
		else {
			throw new RuntimeException("Vertex values have to be specified through a vertices input file"
				+ "or a user-defined map function.");
		}
	}

	/**
	 * Creates a Graph from CSV input with edge values, but without vertex values.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <EV> the edge value type
	 *
	 * @param topVertexKey the type of the top vertex IDs
	 * @param bottomVertexKey the type of the bottom vertex IDs
	 * @param edgeValue the type of the edge values
	 * @return a Graph where the edges are read from an edges CSV file (with values).
	 */
	public <KT, KB, EV> BipartiteGraph<KT, KB, NullValue, NullValue, EV> edgeTypes(
			Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<EV> edgeValue) {

		if (edgeReader == null) {
			throw new RuntimeException("The edges input file cannot be null!");
		}

		DataSet<Tuple3<KT, KB, EV>> edges = edgeReader.types(topVertexKey, bottomVertexKey, edgeValue);

		return BipartiteGraph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 * Creates a Graph from CSV input without vertex values or edge values.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 *
	 * @param topVertexKey the type of the top vertex IDs
	 * @param bottomVertexKey the type of the bottom vertex IDs
	 * @return a Graph where the vertex IDs are read from the edges input file.
	 */
	public <KT, KB> BipartiteGraph<KT, KB, NullValue, NullValue, NullValue> keyType(
			Class<KT> topVertexKey, Class<KB> bottomVertexKey) {

		if (edgeReader == null) {
			throw new RuntimeException("The edges input file cannot be null!");
		}

		DataSet<Tuple3<KT, KB, NullValue>> edges = edgeReader.types(topVertexKey, bottomVertexKey)
			.map(new Tuple2ToTuple3Map<KT, KB>());

		return BipartiteGraph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 * Creates a Graph from CSV input without edge values.
	 * The vertex values are specified through a vertices input file or a user-defined map function.
	 * If no vertices input file is provided, the vertex IDs are automatically created from the edges
	 * input file.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 *
	 * @param topVertexKey the type of the top vertex IDs
	 * @param bottomVertexKey the type of the bottom vertex IDs
	 * @param topVertexValue the type of the top vertex values
	 * @param bottomVertexValue the type of the bottom vertex values
	 * @return a Graph where the vertex IDs and vertex values.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public <KT, KB, VVT, VVB> BipartiteGraph<KT, KB, VVT, VVB, NullValue> vertexTypes(
			Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<VVT> topVertexValue, Class<VVB> bottomVertexValue) {

		if (edgeReader == null) {
			throw new RuntimeException("The edges input file cannot be null!");
		}
		DataSet<Tuple3<KT, KB, NullValue>> edges = edgeReader.types(topVertexKey, bottomVertexKey)
			.map(new Tuple2ToTuple3Map<KT, KB>());

		// the vertex value can be provided by an input file or a user-defined mapper
		if (topVertexReader != null && bottomVertexReader != null) {
			DataSet<Tuple2<KT, VVT>> topVertices = topVertexReader.types(topVertexKey, topVertexValue);
			DataSet<Tuple2<KB, VVB>> bottomVertices = bottomVertexReader.types(bottomVertexKey, bottomVertexValue);
			return BipartiteGraph.fromTupleDataSet(topVertices, bottomVertices, edges, executionContext);
		}
		else if (topMapper != null && bottomMapper != null) {
			return BipartiteGraph.fromTupleDataSet(
				edges, (MapFunction<KT, VVT>) topMapper, (MapFunction<KB, VVB>) bottomMapper, executionContext);
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
	public BipartiteGraphCsvReader lineDelimiterEdges(String delimiter) {
		edgeReader.lineDelimiter(delimiter);
		return this;
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the top vertices
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader lineDelimiterTopVertices(String delimiter) {
		if (topVertexReader != null) {
			topVertexReader.lineDelimiter(delimiter);
		}
		return this;
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the bottom vertices
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader lineDelimiterBottomVertices(String delimiter) {
		if (bottomVertexReader != null) {
			bottomVertexReader.lineDelimiter(delimiter);
		}
		return this;
	}

	/**
	 *Configures the Delimiter that separates fields in a row for the CSV reader used to read the top vertices
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader fieldDelimiterTopVertices(String delimiter) {
		if (topVertexReader != null) {
			topVertexReader.fieldDelimiter(delimiter);
		}
		return this;
	}

	/**
	 *Configures the Delimiter that separates fields in a row for the CSV reader used to read the top vertices
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader fieldDelimiterBottomVertices(String delimiter) {
		if (bottomVertexReader != null) {
			bottomVertexReader.fieldDelimiter(delimiter);
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
	public BipartiteGraphCsvReader fieldDelimiterEdges(String delimiter) {
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
	public BipartiteGraphCsvReader parseQuotedStringsEdges(char quoteCharacter) {
		this.edgeReader.parseQuotedStrings(quoteCharacter);
		return this;
	}

	/**
	 * Enables quoted String parsing for top vertices Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader parseQuotedStringsTopVertices(char quoteCharacter) {
		if (topVertexReader != null) {
			topVertexReader.parseQuotedStrings(quoteCharacter);
		}
		return this;
	}

	/**
	 * Enables quoted String parsing for bottom vertices Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader parseQuotedStringsBottomVertices(char quoteCharacter) {
		if (bottomVertexReader != null) {
			bottomVertexReader.parseQuotedStrings(quoteCharacter);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the top vertices Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreCommentsTopVertices(String commentPrefix) {
		if (topVertexReader != null) {
			topVertexReader.ignoreComments(commentPrefix);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the bottom vertices Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreCommentsBottomVertices(String commentPrefix) {
		if (bottomVertexReader != null) {
			bottomVertexReader.ignoreComments(commentPrefix);
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
	public BipartiteGraphCsvReader ignoreCommentsEdges(String commentPrefix) {
		this.edgeReader.ignoreComments(commentPrefix);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing top vertices data should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 *
	 * @param vertexFields The array of flags that describes which fields are to be included from the CSV file for top
	 *                     vertices.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader includeFieldsTopVertices(boolean... vertexFields) {
		if (topVertexReader != null) {
			topVertexReader.includeFields(vertexFields);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing top vertices data should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 *
	 * @param vertexFields The array of flags that describes which fields are to be included from the CSV file for top
	 *                     vertices.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader includeFieldsBottomVertices(boolean... vertexFields) {
		if (bottomVertexReader != null) {
			bottomVertexReader.includeFields(vertexFields);
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
	public BipartiteGraphCsvReader includeFieldsEdges(boolean... edgeFields) {
		this.edgeReader.includeFields(edgeFields);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing top vertices data should be included and which should be skipped. The
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
	public BipartiteGraphCsvReader includeFieldsTopVertices(String mask) {
		if (topVertexReader != null) {
			topVertexReader.includeFields(mask);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing bottom vertices data should be included and which should be skipped. The
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
	public BipartiteGraphCsvReader includeFieldsBottomVertices(String mask) {
		if (bottomVertexReader != null) {
			bottomVertexReader.includeFields(mask);
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
	public BipartiteGraphCsvReader includeFieldsEdges(String mask) {
		this.edgeReader.includeFields(mask);
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing top vertices data should be included and which should be skipped. The
	 * bits in the value (read from least significant to most significant) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the position of the most significant
	 * non-zero bit.
	 * The parser will skip over all fields where the character at the corresponding bit is zero, and
	 * include the fields where the corresponding bit is one.
	 * <p>
	 * Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader includeFieldsTopVertices(long mask) {
		if (topVertexReader != null) {
			topVertexReader.includeFields(mask);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file containing bottom vertices data should be included and which should be skipped. The
	 * bits in the value (read from least significant to most significant) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the position of the most significant
	 * non-zero bit.
	 * The parser will skip over all fields where the character at the corresponding bit is zero, and
	 * include the fields where the corresponding bit is one.
	 * <p>
	 * Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader includeFieldsBottomVertices(long mask) {
		if (bottomVertexReader != null) {
			bottomVertexReader.includeFields(mask);
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
	 * <p>
	 * Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader includeFieldsEdges(long mask) {
		this.edgeReader.includeFields(mask);
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreFirstLineEdges() {
		this.edgeReader.ignoreFirstLine();
		return this;
	}

	/**
	 * Sets the CSV reader for the top vertices file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreFirstLineTopVertices() {
		if (topVertexReader != null) {
			topVertexReader.ignoreFirstLine();
		}
		return this;
	}

	/**
	 * Sets the CSV reader for the bottom vertices file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreFirstLineBottomVertices() {
		if (bottomVertexReader != null) {
			bottomVertexReader.ignoreFirstLine();
		}
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreInvalidLinesEdges() {
		this.edgeReader.ignoreInvalidLines();
		return this;
	}

	/**
	 * Sets the CSV reader top vertices file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreInvalidLinesTopVertices() {
		if (topVertexReader != null) {
			topVertexReader.ignoreInvalidLines();
		}
		return this;
	}

	/**
	 * Sets the CSV reader bottom vertices file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The GraphCSVReader instance itself, to allow for fluent function chaining.
	 */
	public BipartiteGraphCsvReader ignoreInvalidLinesBottomVertices() {
		if (bottomVertexReader != null) {
			bottomVertexReader.ignoreInvalidLines();
		}
		return this;
	}
}
