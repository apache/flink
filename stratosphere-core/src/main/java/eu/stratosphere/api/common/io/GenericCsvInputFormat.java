/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.types.Value;
import eu.stratosphere.types.parser.FieldParser;
import eu.stratosphere.util.InstantiationUtil;


public abstract class GenericCsvInputFormat<OT> extends DelimitedInputFormat<OT> {
	
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unchecked")
	private static final Class<? extends Value>[] EMPTY_TYPES = new Class[0];
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];
	
	private static final char DEFAULT_FIELD_DELIMITER = ',';
	
	
	// --------------------------------------------------------------------------------------------
	//  Variables for internal operation.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------

	private transient FieldParser<Value>[] fieldParsers;
	
	
	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------
	
	private Class<? extends Value>[] fieldTypes = EMPTY_TYPES;
	
	private boolean[] fieldIncluded = EMPTY_INCLUDED;
		
	private char fieldDelim = DEFAULT_FIELD_DELIMITER;
	
	private boolean lenient;
	
	private boolean skipFirstLineAsHeader;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------
	
	public GenericCsvInputFormat() {
		this(DEFAULT_FIELD_DELIMITER);
	}
	
	public GenericCsvInputFormat(char fieldDelimiter) {
		setFieldDelim(fieldDelimiter);
	}
	
	public GenericCsvInputFormat(Class<? extends Value> ... fields) {
		this(DEFAULT_FIELD_DELIMITER, fields);
	}
	
	public GenericCsvInputFormat(char fieldDelimiter, Class<? extends Value> ... fields) {
		setFieldDelim(fieldDelimiter);
		setFieldTypes(fields);
	}
	
	// --------------------------------------------------------------------------------------------

	public Class<? extends Value>[] getFieldTypes() {
		// check if we are dense, ie, we read all fields
		if (this.fieldIncluded.length == this.fieldTypes.length) {
			return this.fieldTypes;
		}
		else {
			// sparse type array which we made dense for internal bookkeeping.
			// create a sparse copy to return
			@SuppressWarnings("unchecked")
			Class<? extends Value>[] types = new Class[this.fieldIncluded.length];
			
			for (int i = 0, k = 0; i < this.fieldIncluded.length; i++) {
				if (this.fieldIncluded[i]) {
					types[i] = this.fieldTypes[k++];
				}
			}
			
			return types;
		}
	}
	
	public void setFieldTypesArray(Class<? extends Value>[] fieldTypes) {
		setFieldTypes(fieldTypes);
	}

	public void setFieldTypes(Class<? extends Value> ... fieldTypes) {
		if (fieldTypes == null)
			throw new IllegalArgumentException("Field types must not be null.");
		
		this.fieldIncluded = new boolean[fieldTypes.length];
		ArrayList<Class<? extends Value>> types = new ArrayList<Class<? extends Value>>();
		
		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<? extends Value> type = fieldTypes[i];
			
			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[i] = true;
			}
		}
		
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] denseTypeArray = (Class<? extends Value>[]) types.toArray(new Class[types.size()]);
		this.fieldTypes = denseTypeArray;
	}

	public void setFields(int[] sourceFieldIndices, Class<? extends Value>[] fieldTypes) {
		Preconditions.checkNotNull(fieldTypes);
		Preconditions.checkArgument(sourceFieldIndices.length == fieldTypes.length,
			"Number of field indices and field types must match.");

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		int largestFieldIndex = Ints.max(sourceFieldIndices);
		this.fieldIncluded = new boolean[largestFieldIndex + 1];
		ArrayList<Class<? extends Value>> types = new ArrayList<Class<? extends Value>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<? extends Value> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName()
						+ "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[sourceFieldIndices[i]] = true;
			}
		}

		@SuppressWarnings("unchecked")
		Class<? extends Value>[] denseTypeArray = (Class<? extends Value>[]) types.toArray(new Class[types.size()]);
		this.fieldTypes = denseTypeArray;
	}
	
	public int getNumberOfFieldsTotal() {
		return this.fieldIncluded.length;
	}
	
	public int getNumberOfNonNullFields() {
		return this.fieldTypes.length;
	}

	public char getFieldDelim() {
		return fieldDelim;
	}

	public void setFieldDelim(char fieldDelim) {
		if (fieldDelim > Byte.MAX_VALUE)
			throw new IllegalArgumentException("The field delimiter must be an ASCII character.");
		
		this.fieldDelim = fieldDelim;
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
	
	protected FieldParser<Value>[] getFieldParsers() {
		return this.fieldParsers;
	}

	// --------------------------------------------------------------------------------------------
	//  Runtime methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		// instantiate the parsers
		@SuppressWarnings("unchecked")
		FieldParser<Value>[] parsers = new FieldParser[fieldTypes.length];
		
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}
				
				@SuppressWarnings("unchecked")
				FieldParser<Value> p = (FieldParser<Value>) InstantiationUtil.instantiate(parserType, FieldParser.class);
				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
		
		// skip the first line, if we are at the beginning of a file and have the option set
		if (this.skipFirstLineAsHeader && this.splitStart == 0) {
			readLine(); // read and ignore
		}
	}
	
	protected boolean parseRecord(Value[] valueHolders, byte[] bytes, int offset, int numBytes) throws ParseException {
		
		boolean[] fieldIncluded = this.fieldIncluded;
		
		int startPos = offset;
		final int limit = offset + numBytes;
		
		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {
			
			// check valid start position
			if (startPos >= limit) {
				if (lenient) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
				}
			}
			
			if (fieldIncluded[field]) {
				// parse field
				FieldParser<Value> parser = this.fieldParsers[output];
				Value val = valueHolders[output];
				
				startPos = parser.parseField(bytes, startPos, limit, this.fieldDelim, val);
				
				// check parse result
				if (startPos < 0) {
					// no good
					if (lenient) {
						return false;
					} else {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: " + lineAsString);
					}
				}
				output++;
			}
			else {
				// skip field(s)
				int skipCnt = 1;
				while (!fieldIncluded[field + skipCnt]) {
					skipCnt++;
				}
				startPos = skipFields(bytes, startPos, limit, fieldDelim, skipCnt);
				if (startPos >= 0) {
					field += (skipCnt - 1);
				}
				else if (lenient) {
					// no valid line, but we do not report an exception, simply skip the line
					return false;
				}
				else {
					String lineAsString = new String(bytes, offset, numBytes);
					throw new ParseException("Line could not be parsed: " + lineAsString);
				}
			}
		}
		return true;
	}
	
	protected int skipFields(byte[] bytes, int startPos, int limit, char delim, int skipCnt) {
		int i = startPos;
		
		while (i < limit && skipCnt > 0) {
			if (bytes[i] == delim) {
				skipCnt--;
			}
			i++;
		}
		if (skipCnt == 0) {
			return i;
		} else {
			return -1;
		}
	}
}