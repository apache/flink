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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

//CHECKSTYLE.OFF: AvoidStarImport|ImportOrder
import org.apache.flink.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport|ImportOrder

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A builder class to instantiate a CSV parsing data source. The CSV reader configures the field types,
 * the delimiters (row and field),  the fields that should be included or skipped, and other flags
 * such as whether to skip the initial line as the header.
 */
@Public
public class CsvReader {

	private final Path path;

	private final ExecutionEnvironment executionContext;

	protected boolean[] includedMask;

	protected String lineDelimiter = CsvInputFormat.DEFAULT_LINE_DELIMITER;

	protected String fieldDelimiter = CsvInputFormat.DEFAULT_FIELD_DELIMITER;

	protected String commentPrefix = null; //default: no comments

	protected boolean parseQuotedStrings = false;

	protected char quoteCharacter = '"';

	protected boolean skipFirstLineAsHeader = false;

	protected boolean ignoreInvalidLines = false;

	private String charset = "UTF-8";

	// --------------------------------------------------------------------------------------------

	public CsvReader(Path filePath, ExecutionEnvironment executionContext) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		Preconditions.checkNotNull(executionContext, "The execution context may not be null.");

		this.path = filePath;
		this.executionContext = executionContext;
	}

	public CsvReader(String filePath, ExecutionEnvironment executionContext) {
		this(new Path(Preconditions.checkNotNull(filePath, "The file path may not be null.")), executionContext);
	}

	public Path getFilePath() {
		return this.path;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Configures the delimiter that separates the lines/rows. The linebreak character
	 * ({@code '\n'}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the rows.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader lineDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}

		this.lineDelimiter = delimiter;
		return this;
	}

	/**
	 * Configures the delimiter that separates the fields within a row. The comma character
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in one row.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 *
	 * @deprecated Please use {@link #fieldDelimiter(String)}.
	 */
	@Deprecated
	@PublicEvolving
	public CsvReader fieldDelimiter(char delimiter) {
		this.fieldDelimiter = String.valueOf(delimiter);
		return this;
	}

	/**
	 * Configures the delimiter that separates the fields within a row. The comma character
	 * ({@code ','}) is used by default.
	 *
	 * @param delimiter The delimiter that separates the fields in one row.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader fieldDelimiter(String delimiter) {
		this.fieldDelimiter = delimiter;
		return this;
	}

	/**
	 * Enables quoted String parsing. Field delimiters in quoted Strings are ignored.
	 * A String is parsed as quoted if it starts and ends with a quoting character and as unquoted otherwise.
	 * Leading or tailing whitespaces are not allowed.
	 *
	 * @param quoteCharacter The character which is used as quoting character.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader parseQuotedStrings(char quoteCharacter) {
		this.parseQuotedStrings = true;
		this.quoteCharacter = quoteCharacter;
		return this;
	}

	/**
	 * Configures the string that starts comments.
	 * By default comments will be treated as invalid lines.
	 * This function only recognizes comments which start at the beginning of the line!
	 *
	 * @param commentPrefix The string that starts the comments.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader ignoreComments(String commentPrefix) {
		if (commentPrefix == null || commentPrefix.length() == 0) {
			throw new IllegalArgumentException("The comment prefix must not be null or an empty string");
		}

		this.commentPrefix = commentPrefix;
		return this;
	}

	/**
	 * Gets the character set for the reader. Default is UTF-8.
	 *
	 * @return The charset for the reader.
	 */
	@PublicEvolving
	public String getCharset() {
		return this.charset;
	}

	/**
	 * Sets the charset of the reader.
	 *
	 * @param charset The character set to set.
	 */
	@PublicEvolving
	public void setCharset(String charset) {
		this.charset = Preconditions.checkNotNull(charset);
	}

	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 *
	 * @param fields The array of flags that describes which fields are to be included and which not.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader includeFields(boolean ... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("The set of included fields must not be null or empty.");
		}

		int lastTruePos = -1;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i]) {
				lastTruePos = i;
			}
		}

		if (lastTruePos == -1) {
			throw new IllegalArgumentException("The description of fields to parse excluded all fields. At least one fields must be included.");
		}
		if (lastTruePos == fields.length - 1) {
			this.includedMask = fields;
		} else {
			this.includedMask = Arrays.copyOfRange(fields, 0, lastTruePos + 1);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
	 * positions in the string (read from position 0 to its length) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the mask string
	 * The parser will skip over all fields where the character at the corresponding position
	 * in the string is {@code '0'}, {@code 'F'}, or {@code 'f'} (representing the value
	 * {@code false}). The result contains the fields where the corresponding position in
	 * the boolean array is {@code '1'}, {@code 'T'}, or {@code 't'} (representing the value {@code true}).
	 *
	 * @param mask The string mask defining which fields to include and which to skip.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader includeFields(String mask) {
		boolean[] includedMask = new boolean[mask.length()];

		for (int i = 0; i < mask.length(); i++) {
			char c = mask.charAt(i);
			if (c == '1' || c == 'T' || c == 't') {
				includedMask[i] = true;
			} else if (c != '0' && c != 'F' && c != 'f') {
				throw new IllegalArgumentException("Mask string may contain only '0' and '1'.");
			}
		}

		return includeFields(includedMask);
	}

	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
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
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader includeFields(long mask) {
		if (mask == 0) {
			throw new IllegalArgumentException("The description of fields to parse excluded all fields. At least one fields must be included.");
		}

		ArrayList<Boolean> fields = new ArrayList<Boolean>();

		while (mask != 0) {
			fields.add((mask & 0x1L) != 0);
			mask >>>= 1;
		}

		boolean[] fieldsArray = new boolean[fields.size()];
		for (int i = 0; i < fieldsArray.length; i++) {
			fieldsArray[i] = fields.get(i);
		}

		return includeFields(fieldsArray);
	}

	/**
	 * Sets the CSV reader to ignore the first line. This is useful for files that contain a header line.
	 *
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader ignoreFirstLine() {
		skipFirstLineAsHeader = true;
		return this;
	}

	/**
	 * Sets the CSV reader to ignore any invalid lines.
	 * This is useful for files that contain an empty line at the end, multiple header lines or comments. This would throw an exception otherwise.
	 *
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public CsvReader ignoreInvalidLines(){
		ignoreInvalidLines = true;
		return this;
	}

	/**
	 * Configures the reader to read the CSV data and parse it to the given type. The all fields of the type
	 * must be public or able to set value. The type information for the fields is obtained from the type class.
	 *
	 * @param pojoType The class of the target POJO.
	 * @param pojoFields The fields of the POJO which are mapped to CSV fields.
	 * @return The DataSet representing the parsed CSV data.
	 */
	public <T> DataSource<T> pojoType(Class<T> pojoType, String... pojoFields) {
		Preconditions.checkNotNull(pojoType, "The POJO type class must not be null.");
		Preconditions.checkArgument(pojoFields != null && pojoFields.length > 0, "POJO fields must be specified (not null) if output type is a POJO.");

		final TypeInformation<T> ti = TypeExtractor.createTypeInfo(pojoType);
		if (!(ti instanceof PojoTypeInfo)) {
			throw new IllegalArgumentException(
				"The specified class is not a POJO. The type class must meet the POJO requirements. Found: " + ti);
		}
		final PojoTypeInfo<T> pti = (PojoTypeInfo<T>) ti;

		CsvInputFormat<T> inputFormat = new PojoCsvInputFormat<T>(path, this.lineDelimiter, this.fieldDelimiter, pti, pojoFields, this.includedMask);

		configureInputFormat(inputFormat);

		return new DataSource<T>(executionContext, inputFormat, pti, Utils.getCallLocationName());
	}

	/**
	 * Configures the reader to read the CSV data and parse it to the given type. The type must be a subclass of
	 * {@link Tuple}. The type information for the fields is obtained from the type class. The type
	 * consequently needs to specify all generic field types of the tuple.
	 *
	 * @param targetType The class of the target type, needs to be a subclass of Tuple.
	 * @return The DataSet representing the parsed CSV data.
	 */
	public <T extends Tuple> DataSource<T> tupleType(Class<T> targetType) {
		Preconditions.checkNotNull(targetType, "The target type class must not be null.");
		if (!Tuple.class.isAssignableFrom(targetType)) {
			throw new IllegalArgumentException("The target type must be a subclass of " + Tuple.class.getName());
		}

		@SuppressWarnings("unchecked")
		TupleTypeInfo<T> typeInfo = (TupleTypeInfo<T>) TypeExtractor.createTypeInfo(targetType);
		CsvInputFormat<T> inputFormat = new TupleCsvInputFormat<T>(path, this.lineDelimiter, this.fieldDelimiter, typeInfo, this.includedMask);

		Class<?>[] classes = new Class<?>[typeInfo.getArity()];
		for (int i = 0; i < typeInfo.getArity(); i++) {
			classes[i] = typeInfo.getTypeAt(i).getTypeClass();
		}

		configureInputFormat(inputFormat);
		return new DataSource<T>(executionContext, inputFormat, typeInfo, Utils.getCallLocationName());
	}

	// --------------------------------------------------------------------------------------------
	// Miscellaneous
	// --------------------------------------------------------------------------------------------

	private void configureInputFormat(CsvInputFormat<?> format) {
		format.setCharset(this.charset);
		format.setDelimiter(this.lineDelimiter);
		format.setFieldDelimiter(this.fieldDelimiter);
		format.setCommentPrefix(this.commentPrefix);
		format.setSkipFirstLineAsHeader(skipFirstLineAsHeader);
		format.setLenient(ignoreInvalidLines);
		if (this.parseQuotedStrings) {
			format.enableQuotedStringParsing(this.quoteCharacter);
		}
	}

	// --------------------------------------------------------------------------------------------
	// The following lines are generated.
	// --------------------------------------------------------------------------------------------
	// BEGIN_OF_TUPLE_DEPENDENT_CODE
	// GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 1-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0> DataSource<Tuple1<T0>> types(Class<T0> type0) {
		TupleTypeInfo<Tuple1<T0>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0);
		CsvInputFormat<Tuple1<T0>> inputFormat = new TupleCsvInputFormat<Tuple1<T0>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple1<T0>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 2-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1> DataSource<Tuple2<T0, T1>> types(Class<T0> type0, Class<T1> type1) {
		TupleTypeInfo<Tuple2<T0, T1>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1);
		CsvInputFormat<Tuple2<T0, T1>> inputFormat = new TupleCsvInputFormat<Tuple2<T0, T1>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple2<T0, T1>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 3-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2> DataSource<Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
		TupleTypeInfo<Tuple3<T0, T1, T2>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2);
		CsvInputFormat<Tuple3<T0, T1, T2>> inputFormat = new TupleCsvInputFormat<Tuple3<T0, T1, T2>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple3<T0, T1, T2>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 4-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3> DataSource<Tuple4<T0, T1, T2, T3>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
		TupleTypeInfo<Tuple4<T0, T1, T2, T3>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3);
		CsvInputFormat<Tuple4<T0, T1, T2, T3>> inputFormat = new TupleCsvInputFormat<Tuple4<T0, T1, T2, T3>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple4<T0, T1, T2, T3>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 5-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4> DataSource<Tuple5<T0, T1, T2, T3, T4>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
		TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4);
		CsvInputFormat<Tuple5<T0, T1, T2, T3, T4>> inputFormat = new TupleCsvInputFormat<Tuple5<T0, T1, T2, T3, T4>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple5<T0, T1, T2, T3, T4>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 6-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5> DataSource<Tuple6<T0, T1, T2, T3, T4, T5>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
		TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5);
		CsvInputFormat<Tuple6<T0, T1, T2, T3, T4, T5>> inputFormat = new TupleCsvInputFormat<Tuple6<T0, T1, T2, T3, T4, T5>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple6<T0, T1, T2, T3, T4, T5>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 7-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6> DataSource<Tuple7<T0, T1, T2, T3, T4, T5, T6>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
		TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6);
		CsvInputFormat<Tuple7<T0, T1, T2, T3, T4, T5, T6>> inputFormat = new TupleCsvInputFormat<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 8-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7> DataSource<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
		TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7);
		CsvInputFormat<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> inputFormat = new TupleCsvInputFormat<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 9-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8> DataSource<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
		TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8);
		CsvInputFormat<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> inputFormat = new TupleCsvInputFormat<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 10-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> DataSource<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
		TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9);
		CsvInputFormat<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> inputFormat = new TupleCsvInputFormat<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 11-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> DataSource<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10) {
		TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10);
		CsvInputFormat<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> inputFormat = new TupleCsvInputFormat<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 12-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> DataSource<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11) {
		TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11);
		CsvInputFormat<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> inputFormat = new TupleCsvInputFormat<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 13-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> DataSource<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12) {
		TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12);
		CsvInputFormat<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> inputFormat = new TupleCsvInputFormat<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 14-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> DataSource<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
		TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13);
		CsvInputFormat<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> inputFormat = new TupleCsvInputFormat<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 15-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> DataSource<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14) {
		TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14);
		CsvInputFormat<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> inputFormat = new TupleCsvInputFormat<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 16-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> DataSource<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15) {
		TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15);
		CsvInputFormat<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> inputFormat = new TupleCsvInputFormat<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 17-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> DataSource<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16) {
		TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16);
		CsvInputFormat<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> inputFormat = new TupleCsvInputFormat<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 18-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> DataSource<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
		TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17);
		CsvInputFormat<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> inputFormat = new TupleCsvInputFormat<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 19-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> DataSource<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18) {
		TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18);
		CsvInputFormat<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> inputFormat = new TupleCsvInputFormat<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 20-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> DataSource<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19) {
		TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19);
		CsvInputFormat<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> inputFormat = new TupleCsvInputFormat<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 21-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @param type20 The type of CSV field 20 and the type of field 20 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> DataSource<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20) {
		TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20);
		CsvInputFormat<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> inputFormat = new TupleCsvInputFormat<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 22-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @param type20 The type of CSV field 20 and the type of field 20 in the returned tuple type.
	 * @param type21 The type of CSV field 21 and the type of field 21 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> DataSource<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
		TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21);
		CsvInputFormat<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> inputFormat = new TupleCsvInputFormat<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 23-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @param type20 The type of CSV field 20 and the type of field 20 in the returned tuple type.
	 * @param type21 The type of CSV field 21 and the type of field 21 in the returned tuple type.
	 * @param type22 The type of CSV field 22 and the type of field 22 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> DataSource<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22) {
		TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22);
		CsvInputFormat<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> inputFormat = new TupleCsvInputFormat<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 24-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @param type20 The type of CSV field 20 and the type of field 20 in the returned tuple type.
	 * @param type21 The type of CSV field 21 and the type of field 21 in the returned tuple type.
	 * @param type22 The type of CSV field 22 and the type of field 22 in the returned tuple type.
	 * @param type23 The type of CSV field 23 and the type of field 23 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> DataSource<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23) {
		TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23);
		CsvInputFormat<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> inputFormat = new TupleCsvInputFormat<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 25-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @param type1 The type of CSV field 1 and the type of field 1 in the returned tuple type.
	 * @param type2 The type of CSV field 2 and the type of field 2 in the returned tuple type.
	 * @param type3 The type of CSV field 3 and the type of field 3 in the returned tuple type.
	 * @param type4 The type of CSV field 4 and the type of field 4 in the returned tuple type.
	 * @param type5 The type of CSV field 5 and the type of field 5 in the returned tuple type.
	 * @param type6 The type of CSV field 6 and the type of field 6 in the returned tuple type.
	 * @param type7 The type of CSV field 7 and the type of field 7 in the returned tuple type.
	 * @param type8 The type of CSV field 8 and the type of field 8 in the returned tuple type.
	 * @param type9 The type of CSV field 9 and the type of field 9 in the returned tuple type.
	 * @param type10 The type of CSV field 10 and the type of field 10 in the returned tuple type.
	 * @param type11 The type of CSV field 11 and the type of field 11 in the returned tuple type.
	 * @param type12 The type of CSV field 12 and the type of field 12 in the returned tuple type.
	 * @param type13 The type of CSV field 13 and the type of field 13 in the returned tuple type.
	 * @param type14 The type of CSV field 14 and the type of field 14 in the returned tuple type.
	 * @param type15 The type of CSV field 15 and the type of field 15 in the returned tuple type.
	 * @param type16 The type of CSV field 16 and the type of field 16 in the returned tuple type.
	 * @param type17 The type of CSV field 17 and the type of field 17 in the returned tuple type.
	 * @param type18 The type of CSV field 18 and the type of field 18 in the returned tuple type.
	 * @param type19 The type of CSV field 19 and the type of field 19 in the returned tuple type.
	 * @param type20 The type of CSV field 20 and the type of field 20 in the returned tuple type.
	 * @param type21 The type of CSV field 21 and the type of field 21 in the returned tuple type.
	 * @param type22 The type of CSV field 22 and the type of field 22 in the returned tuple type.
	 * @param type23 The type of CSV field 23 and the type of field 23 in the returned tuple type.
	 * @param type24 The type of CSV field 24 and the type of field 24 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> DataSource<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23, Class<T24> type24) {
		TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23, type24);
		CsvInputFormat<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> inputFormat = new TupleCsvInputFormat<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(path, types, this.includedMask);
		configureInputFormat(inputFormat);
		return new DataSource<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(executionContext, inputFormat, types, Utils.getCallLocationName());
	}

	// END_OF_TUPLE_DEPENDENT_CODE
}
