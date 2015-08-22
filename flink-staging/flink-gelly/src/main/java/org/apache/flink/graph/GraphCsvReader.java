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
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.NullValue;
/**
 * A class to build a Graph using path(s) provided to CSV file(s) with edge (vertices) data
 * The class also configures the CSV readers used to read edges(vertices) data such as the field types,
 * the delimiters (row and field),  the fields that should be included or skipped, and other flags
 * such as whether to skip the initial line as the header.
 * The configuration is done using the functions provided in The {@link org.apache.flink.api.java.io.CsvReader} class.
 */
@SuppressWarnings({"unused" , "unchecked"})
public class GraphCsvReader<K,VV,EV> {

	private final Path vertexPath,edgePath;
	private final ExecutionEnvironment executionContext;
	protected CsvReader EdgeReader;
	protected CsvReader VertexReader;
	protected MapFunction<K, VV> mapper;
	protected Class<K> vertexKey;
	protected Class<VV> vertexValue;
	protected Class<EV> edgeValue;

//--------------------------------------------------------------------------------------------------------------------
	public GraphCsvReader(Path vertexPath,Path edgePath, ExecutionEnvironment context) {
		this.vertexPath = vertexPath;
		this.edgePath = edgePath;
		this.VertexReader = new CsvReader(vertexPath,context);
		this.EdgeReader = new CsvReader(edgePath,context);
		this.mapper=null;
		this.executionContext=context;
	}

	public GraphCsvReader(Path edgePath, ExecutionEnvironment context) {
		this.vertexPath = null;
		this.edgePath = edgePath;
		this.EdgeReader = new CsvReader(edgePath,context);
		this.VertexReader = null;
		this.mapper = null;
		this.executionContext=context;
	}

	public GraphCsvReader(Path edgePath,final MapFunction<K, VV> mapper, ExecutionEnvironment context) {
		this.vertexPath = null;
		this.edgePath = edgePath;
		this.EdgeReader = new CsvReader(edgePath,context);
		this.VertexReader = null;
		this.mapper = mapper;
		this.executionContext=context;
	}

	public GraphCsvReader (String edgePath,ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);

	}

	public GraphCsvReader(String vertexPath, String edgePath, ExecutionEnvironment context) {
		this(new Path(Preconditions.checkNotNull(vertexPath, "The file path may not be null.")),
				new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")), context);
	}


	public GraphCsvReader (String edgePath, final MapFunction<K, VV> mapper, ExecutionEnvironment context) {
			this(new Path(Preconditions.checkNotNull(edgePath, "The file path may not be null.")),mapper, context);
	}

	//--------------------------------------------------------------------------------------------------------------------
	/**
	 * Specifies the types for the edges fields and returns this instance of GraphCsvReader
	 *
	 * @param vertexKey The type of Vetex ID in the Graph.
	 * @param  edgeValue The type of Edge Value in the returned Graph.
	 * @return The {@link org.apache.flink.graph.GraphCsvReader}
	 */
	public GraphCsvReader typesEdges(Class<K> vertexKey, Class<EV> edgeValue) {
		this.vertexKey = vertexKey;
		this.edgeValue = edgeValue;
		return this;
	}

	/**
	 * Specifies the types for the edges fields and returns this instance of GraphCsvReader
	 * This method is overloaded for the case when the type of EdgeValue is NullValue
	 * @param vertexKey The type of Vetex ID in the Graph.
	 * @return The {@link org.apache.flink.graph.GraphCsvReader}
	 */
	public GraphCsvReader typesEdges(Class<K> vertexKey) {
		this.vertexKey = vertexKey;
		this.edgeValue = null;
		return this;
	}

	/**
	 * Specifies the types for the vertices fields and returns an instance of Graph
	 * @param vertexKey The type of Vertex ID in the Graph.
	 * @param vertexValue The type of Vertex Value in the Graph.
	 * @return The {@link org.apache.flink.graph.Graph}
	 */
	public Graph<K, VV, EV> typesVertices(Class vertexKey, Class vertexValue) {
		DataSet<Tuple3<K, K, EV>> edges = this.EdgeReader.types(this.vertexKey,this.vertexKey, this.edgeValue);
		if(mapper == null && this.VertexReader != null) {
		DataSet<Tuple2<K, VV>> vertices = this.VertexReader.types(vertexKey, vertexValue);
		return Graph.fromTupleDataSet(vertices, edges, executionContext);
		} else if(this.mapper != null) {
		return Graph.fromTupleDataSet(edges, this.mapper, executionContext);
		} else {
			return null;
		}
	}

	/**
	 * Specifies the types for the vertices fields and returns and instance of Graph
	 * This method is overloaded for the case when vertices don't have a value
	 * @param vertexKey The type of Vertex ID in the Graph.
	 * @return The {@link org.apache.flink.graph.Graph}
	 */
	public Graph<K, NullValue, EV> typesVertices(Class vertexKey) {
		DataSet<Tuple3<K, K, EV>> edges = this.EdgeReader.types(this.vertexKey, this.vertexKey, this.edgeValue);
		return Graph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 * Specifies the types for the vertices fields and returns an instance of Graph when Edges don't have a value
	 * @param vertexKey The type of Vertex ID in the Graph.
	 * @param vertexValue The type of Vertex Value in the Graph.
	 * @return The {@link org.apache.flink.graph.Graph}
	 */
	public Graph<K, VV, NullValue> typesVerticesNullEdge(Class vertexKey, Class vertexValue) {
		DataSet<Tuple3<K, K, NullValue>> edges= this.EdgeReader.types(this.vertexKey, this.vertexKey)
				.map(new MapFunction<Tuple2<K, K>, Tuple3<K, K, NullValue>>() {
					public Tuple3<K, K, NullValue> map(Tuple2<K, K> value) {
						return new Tuple3<K, K, NullValue>(value.f0, value.f1, NullValue.getInstance());
					}
				});
		if(this.mapper == null && this.VertexReader != null) {
		DataSet<Tuple2<K, VV>> vertices = this.VertexReader.types(vertexKey, vertexValue);
		return Graph.fromTupleDataSet(vertices, edges, executionContext);
		} else if (this.mapper != null) {
			return Graph.fromTupleDataSet(edges, mapper, executionContext);
		} else {
		return null;
		}
	}

	/**
	 * Specifies the types for the vertices fields and returns an instance of Graph when Edges don't have a value
	 * This method is overloaded for the case when vertices don't have a value
	 * @param vertexKey The type of Vertex ID in the Graph.
	 * @return The {@link org.apache.flink.graph.Graph}
	 */
	public Graph<K, NullValue, NullValue> typesVerticesNullEdge(Class vertexKey) {
		DataSet<Tuple3<K, K, NullValue>> edges= this.EdgeReader.types(this.vertexKey, this.vertexKey)
				.map(new MapFunction<Tuple2<K, K>, Tuple3<K, K, NullValue>>() {
					public Tuple3<K, K, NullValue> map(Tuple2<K, K> value) {
						return new Tuple3<K, K, NullValue>(value.f0, value.f1, NullValue.getInstance());
					}
				});
		return Graph.fromTupleDataSet(edges, executionContext);
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the edges
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader lineDelimiterEdges(String delimiter) {
		this.EdgeReader.lineDelimiter(delimiter);
		return this;
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV reader used to read the vertices
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader lineDelimiterVertices(String delimiter) {
		if(this.VertexReader !=null) {
			this.VertexReader.lineDelimiter(delimiter);
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
		if(this.VertexReader !=null) {
			this.VertexReader.fieldDelimiter(delimiter);
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
		this.EdgeReader.fieldDelimiter(delimiter);
		return this;
	}

	/**
	 * Enables quoted String parsing for Edge Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The Graph Csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader parseQuotedStringsEdges(char quoteCharacter) {
		this.EdgeReader.parseQuotedStrings(quoteCharacter);
		return this;
	}

	/**
	 * Enables quoted String parsing for Vertex Csv Reader. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The Graph Csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader parseQuotedStringsVertices(char quoteCharacter) {
		if(this.VertexReader !=null) {
			this.VertexReader.parseQuotedStrings(quoteCharacter);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the Vertex Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The Graph csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreCommentsVertices(String commentPrefix) {
		if(this.VertexReader !=null) {
			this.VertexReader.ignoreComments(commentPrefix);
		}
		return this;
	}

	/**
	 * Configures the string that starts comments for the Edge Csv Reader.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The Graph csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreCommentsEdges(String commentPrefix) {
		this.EdgeReader.ignoreComments(commentPrefix);
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
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(boolean ... vertexFields) {
		if(this.VertexReader !=null) {
			this.VertexReader.includeFields(vertexFields);
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
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(boolean ... edgeFields) {
		this.EdgeReader.includeFields(edgeFields);
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
	 * @return The Graph Csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(String mask) {
		if(this.VertexReader !=null) {
			this.VertexReader.includeFields(mask);
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
	 * @return The Graph Csv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(String mask) {
		this.EdgeReader.includeFields(mask);
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
	 * <p>
	 * Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 *
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The Graph CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsVertices(long mask) {
		if(this.VertexReader !=null) {
			this.VertexReader.includeFields(mask);
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
	 * @return The Graph CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader includeFieldsEdges(long mask) {
		this.EdgeReader.includeFields(mask);
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The Graph CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreFirstLineEdges() {
		this.EdgeReader.ignoreFirstLine();
		return this;
	}

	/**
	 * Sets the CSV reader for the Vertices file to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The Graph CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreFirstLineVertices() {
		if(this.VertexReader !=null) {
			this.VertexReader.ignoreFirstLine();
		}
		return this;
	}

	/**
	 * Sets the CSV reader for the Edges file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreInvalidLinesEdges() {
		this.EdgeReader.ignoreInvalidLines();
		return this;
	}

	/**
	 * Sets the CSV reader Vertices file  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreInvalidLinesVertices() {
		if(this.VertexReader !=null) {
			this.VertexReader.ignoreInvalidLines();
		}
		return this;
	}
}