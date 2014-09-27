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

package org.apache.flink.api.scala.operators;


import com.google.common.base.Preconditions;

import org.apache.flink.api.common.io.GenericCsvInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import scala.Product;

public class ScalaCsvInputFormat<OUT extends Product> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;
	
	private transient Object[] parsedValues;
	
	// To speed up readRecord processing. Used to find windows line endings.
	// It is set when open so that readRecord does not have to evaluate it
	private boolean lineDelimiterIsLinebreak = false;

	private final TypeSerializerFactory<OUT> serializerFactory;

	private transient TupleSerializerBase<OUT> serializer;
	
	public ScalaCsvInputFormat(Path filePath, TypeInformation<OUT> typeInfo) {
		super(filePath);

		TypeSerializer<OUT> serializer = typeInfo.createSerializer();
		if (serializer.isStateful()) {
			serializerFactory = new RuntimeStatefulSerializerFactory<OUT>(
					serializer, typeInfo.getTypeClass());
		} else {
			serializerFactory = new RuntimeStatelessSerializerFactory<OUT>(
					serializer, typeInfo.getTypeClass());
		}

		if (!(typeInfo.isTupleType())) {
			throw new UnsupportedOperationException("This only works on tuple types.");
		}
		TupleTypeInfoBase<OUT> tupleType = (TupleTypeInfoBase<OUT>) typeInfo;
		Class[] classes = new Class[tupleType.getArity()];
		for (int i = 0; i < tupleType.getArity(); i++) {
			classes[i] = tupleType.getTypeAt(i).getTypeClass();
		}
		setFieldTypes(classes);
	}

	public void setFieldTypes(Class<?>[] fieldTypes) {
		if (fieldTypes == null || fieldTypes.length == 0) {
			throw new IllegalArgumentException("Field types must not be null or empty.");
		}

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
		
		//throw exception if no field parsers are available
		if (fieldParsers.length == 0) {
			throw new IOException("CsvInputFormat.open(FileInputSplit split) - no field parsers to parse input");
		}
		
		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}
		
		// left to right evaluation makes access [0] okay
		// this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
		if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n' ) {
			this.lineDelimiterIsLinebreak = true;
		}

		serializer = (TupleSerializerBase<OUT>)serializerFactory.getSerializer();
	}

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) {
		/*
		 * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
		 */
		//Find windows end line, so find carriage return before the newline 
		if (this.lineDelimiterIsLinebreak == true && numBytes > 0 && bytes[offset + numBytes -1] == '\r' ) {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}
		
		if (parseRecord(parsedValues, bytes, offset, numBytes)) {
			OUT result = serializer.createInstance(parsedValues);
			return result;
		} else {
			return null;
		}
	}
	
	
	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
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
