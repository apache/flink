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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.StringValueParser;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public abstract class GenericCsvInputFormat<OT> extends DelimitedInputFormat<OT> {

	private static final long serialVersionUID = 1L;
	
	
	private static final Logger LOG = LoggerFactory.getLogger(GenericCsvInputFormat.class);

	private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];
	
	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};

	private static final byte BACKSLASH = 92;

	// --------------------------------------------------------------------------------------------
	//  Variables for internal operation.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------

	private transient FieldParser<?>[] fieldParsers;

	// To speed up readRecord processing. Used to find windows line endings.
	// It is set when open so that readRecord does not have to evaluate it
	protected boolean lineDelimiterIsLinebreak = false;

	protected transient int commentCount;
	protected transient int invalidLineCount;
	
	
	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------
	
	private Class<?>[] fieldTypes = EMPTY_TYPES;
	
	protected boolean[] fieldIncluded = EMPTY_INCLUDED;

	// The byte representation of the delimiter is updated consistent with
	// current charset.
	private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;
	private String fieldDelimString = null;

	private boolean lenient;
	
	private boolean skipFirstLineAsHeader;

	private boolean quotedStringParsing = false;

	private byte quoteCharacter;

	// The byte representation of the comment prefix is updated consistent with
	// current charset.
	protected byte[] commentPrefix = null;
	private String commentPrefixString = null;


	// --------------------------------------------------------------------------------------------
	//  Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	protected GenericCsvInputFormat() {
		super();
	}

	protected GenericCsvInputFormat(Path filePath) {
		super(filePath, null);
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	// --------------------------------------------------------------------------------------------

	public int getNumberOfFieldsTotal() {
		return this.fieldIncluded.length;
	}
	
	public int getNumberOfNonNullFields() {
		return this.fieldTypes.length;
	}

	@Override
	public void setCharset(String charset) {
		super.setCharset(charset);

		if (this.fieldDelimString != null) {
			this.fieldDelim = fieldDelimString.getBytes(getCharset());
		}

		if (this.commentPrefixString != null) {
			this.commentPrefix = commentPrefixString.getBytes(getCharset());
		}
	}

	public byte[] getCommentPrefix() {
		return commentPrefix;
	}

	public void setCommentPrefix(String commentPrefix) {
		if (commentPrefix != null) {
			this.commentPrefix = commentPrefix.getBytes(getCharset());
		} else {
			this.commentPrefix = null;
		}
		this.commentPrefixString = commentPrefix;
	}

	public byte[] getFieldDelimiter() {
		return fieldDelim;
	}

	public void setFieldDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.fieldDelim = delimiter.getBytes(getCharset());
		this.fieldDelimString = delimiter;
	}

	public boolean isLenient() {
		return lenient;
	}

	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}
	
	public boolean isSkippingFirstLineAsHeader() {
		return skipFirstLineAsHeader;
	}

	public void setSkipFirstLineAsHeader(boolean skipFirstLine) {
		this.skipFirstLineAsHeader = skipFirstLine;
	}

	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = (byte)quoteCharacter;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected FieldParser<?>[] getFieldParsers() {
		return this.fieldParsers;
	}
	
	protected Class<?>[] getGenericFieldTypes() {
		// check if we are dense, i.e., we read all fields
		if (this.fieldIncluded.length == this.fieldTypes.length) {
			return this.fieldTypes;
		}
		else {
			// sparse type array which we made dense for internal book keeping.
			// create a sparse copy to return
			Class<?>[] types = new Class<?>[this.fieldIncluded.length];
			
			for (int i = 0, k = 0; i < this.fieldIncluded.length; i++) {
				if (this.fieldIncluded[i]) {
					types[i] = this.fieldTypes[k++];
				}
			}
			
			return types;
		}
	}
	
	
	protected void setFieldTypesGeneric(Class<?> ... fieldTypes) {
		if (fieldTypes == null) {
			throw new IllegalArgumentException("Field types must not be null.");
		}
		
		this.fieldIncluded = new boolean[fieldTypes.length];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();
		
		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];
			
			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[i] = true;
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
	}
	
	protected void setFieldsGeneric(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		checkNotNull(sourceFieldIndices);
		checkNotNull(fieldTypes);
		checkArgument(sourceFieldIndices.length == fieldTypes.length,
			"Number of field indices and field types must match.");

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		int largestFieldIndex = max(sourceFieldIndices);
		this.fieldIncluded = new boolean[largestFieldIndex + 1];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName()
						+ "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[sourceFieldIndices[i]] = true;
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
	}
	
	protected void setFieldsGeneric(boolean[] includedMask, Class<?>[] fieldTypes) {
		checkNotNull(includedMask);
		checkNotNull(fieldTypes);

		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++];

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
					types.add(type);
				}
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
		this.fieldIncluded = includedMask;
	}

	// --------------------------------------------------------------------------------------------
	//  Runtime methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		// instantiate the parsers
		FieldParser<?>[] parsers = new FieldParser<?>[fieldTypes.length];
		
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}

				FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);

				p.setCharset(getCharset());
				if (this.quotedStringParsing) {
					if (p instanceof StringParser) {
						((StringParser)p).enableQuotedStringParsing(this.quoteCharacter);
					} else if (p instanceof StringValueParser) {
						((StringValueParser)p).enableQuotedStringParsing(this.quoteCharacter);
					}
				}

				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
		
		// skip the first line, if we are at the beginning of a file and have the option set
		if (this.skipFirstLineAsHeader && this.splitStart == 0) {
			readLine(); // read and ignore
		}
	}

	@Override
	public void close() throws IOException {
		if (this.invalidLineCount > 0) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("In file \"" + currentSplit.getPath() + "\" (split start: " + this.splitStart + ") " + this.invalidLineCount +" invalid line(s) were skipped.");
			}
		}

		if (this.commentCount > 0) {
			if (LOG.isInfoEnabled()) {
				LOG.info("In file \"" + currentSplit.getPath() + "\" (split start: " + this.splitStart + ") " + this.commentCount +" comment line(s) were skipped.");
			}
		}
		super.close();
	}

	protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		
		boolean[] fieldIncluded = this.fieldIncluded;
		
		int startPos = offset;
		final int limit = offset + numBytes;
		
		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {
			
			// check valid start position
			if (startPos > limit || (startPos == limit && field != fieldIncluded.length - 1)) {
				if (lenient) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes, getCharset()));
				}
			}

			if (fieldIncluded[field]) {
				// parse field
				@SuppressWarnings("unchecked")
				FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
				Object reuse = holders[output];
				startPos = parser.resetErrorStateAndParse(bytes, startPos, limit, this.fieldDelim, reuse);
				holders[output] = parser.getLastResult();

				// check parse result
				if (startPos < 0) {
					// no good
					if (lenient) {
						return false;
					} else {
						String lineAsString = new String(bytes, offset, numBytes, getCharset());
						throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
								+ "ParserError " + parser.getErrorState() + " \n"
								+ "Expect field types: "+fieldTypesToString() + " \n"
								+ "in file: " + currentSplit.getPath());
					}
				}
				else if (startPos == limit
						&& field != fieldIncluded.length - 1
						&& !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
					// We are at the end of the record, but not all fields have been read
					// and the end is not a field delimiter indicating an empty last field.
					if (lenient) {
						return false;
					} else {
						throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
					}
				}
				output++;
			}
			else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
				if (startPos < 0) {
					if (!lenient) {
						String lineAsString = new String(bytes, offset, numBytes, getCharset());
						throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
								+ "Expect field types: "+fieldTypesToString()+" \n"
								+ "in file: " + currentSplit.getPath());
					} else {
						return false;
					}
				}
				else if (startPos == limit
						&& field != fieldIncluded.length - 1
						&& !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
					// We are at the end of the record, but not all fields have been read
					// and the end is not a field delimiter indicating an empty last field.
					if (lenient) {
						return false;
					} else {
						throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
					}
				}
			}
		}
		return true;
	}
	
	private String fieldTypesToString() {
		StringBuilder string = new StringBuilder();
		string.append(this.fieldTypes[0].toString());

		for (int i = 1; i < this.fieldTypes.length; i++) {
			string.append(", ").append(this.fieldTypes[i]);
		}
		
		return string.toString();
	}

	protected int skipFields(byte[] bytes, int startPos, int limit, byte[] delim) {

		int i = startPos;

		final int delimLimit = limit - delim.length + 1;

		if (quotedStringParsing && bytes[i] == quoteCharacter) {

			// quoted string parsing enabled and field is quoted
			// search for ending quote character, continue when it is escaped
			i++;

			while (i < limit && (bytes[i] != quoteCharacter || bytes[i-1] == BACKSLASH)) {
				i++;
			}
			i++;

			if (i == limit) {
				// we are at the end of the record
				return limit;
			} else if ( i < delimLimit && FieldParser.delimiterNext(bytes, i, delim)) {
				// we are not at the end, check if delimiter comes next
				return i + delim.length;
			} else {
				// delimiter did not follow end quote. Error...
				return -1;
			}
		} else {
			// field is not quoted
			while(i < delimLimit && !FieldParser.delimiterNext(bytes, i, delim)) {
				i++;
			}

			if (i >= delimLimit) {
				// no delimiter found. We are at the end of the record
				return limit;
			} else {
				// delimiter found.
				return i + delim.length;
			}
		}
	}

	@SuppressWarnings("unused")
	protected static void checkAndCoSort(int[] positions, Class<?>[] types) {
		if (positions.length != types.length) {
			throw new IllegalArgumentException("The positions and types must be of the same length");
		}

		TreeMap<Integer, Class<?>> map = new TreeMap<Integer, Class<?>>();

		for (int i = 0; i < positions.length; i++) {
			if (positions[i] < 0) {
				throw new IllegalArgumentException("The field " + " (" + positions[i] + ") is invalid.");
			}
			if (types[i] == null) {
				throw new IllegalArgumentException("The type " + i + " is invalid (null)");
			}

			if (map.containsKey(positions[i])) {
				throw new IllegalArgumentException("The position " + positions[i] + " occurs multiple times.");
			}

			map.put(positions[i], types[i]);
		}

		int i = 0;
		for (Map.Entry<Integer, Class<?>> entry : map.entrySet()) {
			positions[i] = entry.getKey();
			types[i] = entry.getValue();
			i++;
		}
	}

	protected static void checkForMonotonousOrder(int[] positions, Class<?>[] types) {
		if (positions.length != types.length) {
			throw new IllegalArgumentException("The positions and types must be of the same length");
		}

		int lastPos = -1;

		for (int i = 0; i < positions.length; i++) {
			if (positions[i] < 0) {
				throw new IllegalArgumentException("The field " + " (" + positions[i] + ") is invalid.");
			}
			if (types[i] == null) {
				throw new IllegalArgumentException("The type " + i + " is invalid (null)");
			}

			if (positions[i] <= lastPos) {
				throw new IllegalArgumentException("The positions must be strictly increasing (no permutations are supported).");
			}

			lastPos = positions[i];
		}
	}
	
	private static int max(int[] ints) {
		checkArgument(ints.length > 0);
		
		int max = ints[0];
		for (int i = 1 ; i < ints.length; i++) {
			max = Math.max(max, ints[i]);
		}
		return max;
	}
}
