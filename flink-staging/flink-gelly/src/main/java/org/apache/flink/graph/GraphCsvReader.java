package org.apache.flink.graph;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.core.fs.Path;
/**
 * Created by shivani on 15/6/15.
 */

/**
 * A class to build a Graph using path(s) provided to CSV file(s) with edge (vertices) data
 * The class also configures the CSV readers used to read edges(vertices) data such as the field types,
 * the delimiters (row and field),  the fields that should be included or skipped, and other flags
 * such as whether to skip the initial line as the header.
 * The configuration is done using the functions provided in The {@link org.apache.flink.api.java.io.CsvReader} class.
 */



public class GraphCsvReader{

	private final Path path1,path2;
	private final ExecutionEnvironment executionContext;

	private Path edgePath;
	private Path vertexPath;
	protected CsvReader EdgeReader;
	protected CsvReader VertexReader;
	protected MapFunction mapper;

//--------------------------------------------------------------------------------------------------------------------

	public GraphCsvReader(Path path1,Path path2, ExecutionEnvironment context)
	{
		this.path1 = path1;
		this.path2 = path2;
		this.VertexReader = new CsvReader(path1,context);
		this.EdgeReader = new CsvReader(path2,context);
		this.mapper=null;
		this.executionContext=context;
	}

	public GraphCsvReader(Path path2, ExecutionEnvironment context)
	{
		this.path1=null;
		this.path2 = path2;
		this.EdgeReader = new CsvReader(path2,context);
		this.VertexReader = null;
		this.mapper = null;
		this.executionContext=context;
	}

	public GraphCsvReader(Path path2,final MapFunction mapper, ExecutionEnvironment context)
	{
		this.path1=null;
		this.path2 = path2;
		this.EdgeReader = new CsvReader(path2,context);
		this.VertexReader = null;
		this.mapper = mapper;
		this.executionContext=context;
	}

	public GraphCsvReader (String path2,ExecutionEnvironment context)
	{
		this(new Path(Preconditions.checkNotNull(path2, "The file path may not be null.")), context);

	}

	public GraphCsvReader(String path1, String path2, ExecutionEnvironment context)
	{
		this(new Path(Preconditions.checkNotNull(path1, "The file path may not be null.")),new Path(Preconditions.checkNotNull(path2, "The file path may not be null.")), context);
	}


	public GraphCsvReader (String path2, final MapFunction mapper, ExecutionEnvironment context)
	{

			this(new Path(Preconditions.checkNotNull(path2, "The file path may not be null.")),mapper, context);


	}

	public CsvReader getEdgeReader()
	{
		return this.EdgeReader;
	}

	public CsvReader getVertexReader()
	{
		return this.VertexReader;
	}
	//--------------------------------------------------------------------------------------------------------------------

	/**
	 * Specifies the types for the Graph fields and returns a Graph with those field types
	 *
	 * This method is overloaded for the case in which Vertices don't have a value
	 *
	 * @param type0 The type of CSV field 0 and 1 for the CSV reader used for reading Edge data, the type of CSV field 0 for the CSV reader used for reading Vertex data  and the type of Vetex ID in the returned Graph.
	 * @param  type1 The type of CSV field 1 for the CSV reader used for reading Vertex data and the type of Vertex Value in the returned Graph.
	 * @param  type2 The type of CSV field 2 for the CSV reader used for reading Edge data and the type of Edge Value in the returned Graph.
	 * @return The {@link org.apache.flink.graph.Graph} with Edges and Vertices extracted from the parsed CSV data.
	 */
	public <K,VV,EV> Graph<K,VV,EV> types(Class<K> type0, Class<VV> type1, Class<EV> type2)
	{
		DataSet<Tuple3<K,K,EV>> edges = this.EdgeReader.types(type0,type0,type2);
		if(path1!=null)
		{
			DataSet<Tuple2<K,VV>> vertices = this.VertexReader.types(type0,type1);
			return Graph.fromTupleDataSet(vertices,edges,executionContext);
		}
		else
		{
			return Graph.fromTupleDataSet(edges,mapper,executionContext);
		}


	}
	/**
	 * Specifies the types for the Graph fields and returns a Graph with those field types
	 *
	 * This method is overloaded for the case in which Vertices don't have a value
	 *
	 * @param type0 The type of CSV field 0 and 1 for the CSV reader used for reading Edge data and the type of Vetex ID in the returned Graph.
	 * @param  type1 The type of CSV field 2 for the CSV reader used for reading Edge data and the type of Edge Value in the returned Graph.
	 * @return The {@link org.apache.flink.graph.Graph} with Edge extracted from the parsed CSV data and Vertices mapped from Edges with null Value.
	 */
	public <K, EV> Graph<K,NullValue,EV> types(Class<K> type0, Class<EV> type1)
	{
		DataSet<Tuple3<K,K,EV>> edges = this.EdgeReader.types(type0,type0,type1);
		return Graph.fromTupleDataSet(edges,executionContext);
	}

	/**
	 *Configures the Delimiter that separates rows for the CSV readers used to read the edges and vertices
	 *	({@code '\n'}) is used by default.
	 *
	 *@param delimiter The delimiter that separates the rows.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader lineDelimiter(String delimiter)
	{
		this.EdgeReader.lineDelimiter(delimiter);
		this.VertexReader.lineDelimiter(delimiter);
		return this;
	}

	/**
	 *Configures the Delimiter that separates fields in a row for the CSV readers used to read the edges and vertices
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	@Deprecated
	public GraphCsvReader fieldDelimiter(char delimiter)
	{
		this.EdgeReader.fieldDelimiter(delimiter);
		this.VertexReader.fieldDelimiter(delimiter);
		return this;
	}


	/**
	 *Configures the Delimiter that separates fields in a row for the CSV readers used to read the edges and vertices
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in a row.
	 * @return The GraphCsv reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader fieldDelimiter(String delimiter)
	{
		this.EdgeReader.fieldDelimiter(delimiter);
		this.VertexReader.fieldDelimiter(delimiter);
		return this;
	}

	/**
	 * Enables quoted String parsing. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The Graph Csv reader instance itself, to allow for fluent function chaining.
	 */

	public GraphCsvReader parseQuotedStrings(char quoteCharacter) {
		this.EdgeReader.parseQuotedStrings(quoteCharacter);
		this.VertexReader.parseQuotedStrings(quoteCharacter);
		return this;
	}

	/**
	 * Configures the string that starts comments.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The Graph csv reader instance itself, to allow for fluent function chaining.
	 */


	public GraphCsvReader ignoreComments(String commentPrefix) {
		this.EdgeReader.ignoreComments(commentPrefix);
		this.VertexReader.ignoreComments(commentPrefix);
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
	public GraphCsvReader includeVertexFields(boolean ... vertexFields) {
		this.VertexReader.includeFields(vertexFields);
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

	public GraphCsvReader includeEdgeFields(boolean ... edgeFields) {
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


	public GraphCsvReader includeVertexFields(String mask) {
		this.VertexReader.includeFields(mask);
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


	public GraphCsvReader includeEdgeFields(String mask) {
		this.VertexReader.includeFields(mask);
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


	public GraphCsvReader includeVertexFields(long mask) {
		this.VertexReader.includeFields(mask);
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


	public GraphCsvReader includeEdgeFields(long mask) {
		this.VertexReader.includeFields(mask);
		return this;
	}



	/**
	 * Sets the CSV readers for the Edges and Vertices files to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The Graph CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreFirstLine() {
		this.EdgeReader.ignoreFirstLine();
		this.VertexReader.ignoreFirstLine();
		return this;
	}

	/**
	 * Sets the CSV readers for the Edges and Vertices files  to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public GraphCsvReader ignoreInvalidLines(){
		this.EdgeReader.ignoreInvalidLines();
		this.VertexReader.ignoreInvalidLines();
		return this;
	}




}
