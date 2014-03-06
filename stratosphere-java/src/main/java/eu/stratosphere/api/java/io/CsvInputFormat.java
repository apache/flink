/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.io.GenericCsvInputFormat;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.parser.FieldParser;
import eu.stratosphere.util.StringUtils;


public class CsvInputFormat<OUT extends Tuple> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;
	
	private static final String DEFAULT_LINE_DELIMITER = "\n";
	
	private static final char DEFAULT_FIELD_DELIMITER = ',';


	private transient Object[] parsedValues;
	
	
	public CsvInputFormat(Path filePath) {
		super(filePath);
	}
	
	
	public CsvInputFormat(Path filePath, Class<?> ... types) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, types);
	}
	
	
	public CsvInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, Class<?> ... types) {
		super(filePath);
		
		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);
		
		setFieldTypes(types);
	}
	
	public void setFieldTypes(Class<?> ... fieldTypes) {
		if (fieldTypes == null || fieldTypes.length == 0)
			throw new IllegalArgumentException("Field types must not be null or empty.");
		
		setFieldTypesGeneric(fieldTypes);
	}

	public void setFields(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldIndices);
		Preconditions.checkNotNull(fieldTypes);
		
		checkForMonotonousOrder(sourceFieldIndices, fieldTypes);
		
		setFieldsGeneric(sourceFieldIndices, fieldTypes);
	}
	
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		@SuppressWarnings("unchecked")
		FieldParser<Object>[] fieldParsers = (FieldParser<Object>[]) getFieldParsers();
		
		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}
	}

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) {
		if (parseRecord(parsedValues, bytes, offset, numBytes)) {
			// valid parse, map values into pact record
			for (int i = 0; i < parsedValues.length; i++) {
				reuse.setField(parsedValues[i], i);
			}
			return reuse;
		} else {
			return null;
		}
	}
	
	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ")";
	}
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unused")
	private static void checkAndCoSort(int[] positions, Class<?>[] types) {
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
	
	private static void checkForMonotonousOrder(int[] positions, Class<?>[] types) {
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
}
