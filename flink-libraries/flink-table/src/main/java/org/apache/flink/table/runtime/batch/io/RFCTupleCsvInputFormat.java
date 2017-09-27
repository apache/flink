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

package org.apache.flink.table.runtime.batch.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.Path;

/**
 * Input format to reads CSV files into Tuples.
 * @param <T> type of Tuple
 */
public class RFCTupleCsvInputFormat<T extends Tuple> extends RFCCsvInputFormat<T> implements ResultTypeQueryable<T> {

	private static final long serialVersionUID = 1L;

	private TypeInformation<T> tupleTypeInfo;
	private TupleSerializerBase<T> tupleSerializer;

	public RFCTupleCsvInputFormat(Path filePath, TupleTypeInfoBase<T> tupleTypeInfo) {
		this(filePath, DEFAULT_RECORD_DELIMITER, DEFAULT_FIELD_DELIMITER, tupleTypeInfo);
	}

	public RFCTupleCsvInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, TupleTypeInfoBase<T> tupleTypeInfo) {
		this(filePath, lineDelimiter, fieldDelimiter, tupleTypeInfo, createDefaultMask(tupleTypeInfo.getArity()));
	}

	public RFCTupleCsvInputFormat(Path filePath, TupleTypeInfoBase<T> tupleTypeInfo, int[] includedFieldsMask) {
		this(filePath, DEFAULT_RECORD_DELIMITER, DEFAULT_FIELD_DELIMITER, tupleTypeInfo, includedFieldsMask);
	}

	public RFCTupleCsvInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, TupleTypeInfoBase<T> tupleTypeInfo, int[] includedFieldsMask) {
		super(filePath);
		boolean[] mask = (includedFieldsMask == null)
				? createDefaultMask(tupleTypeInfo.getArity())
				: toBooleanMask(includedFieldsMask);
		configure(lineDelimiter, fieldDelimiter, tupleTypeInfo, mask);
	}

	public RFCTupleCsvInputFormat(Path filePath, TupleTypeInfoBase<T> tupleTypeInfo, boolean[] includedFieldsMask) {
		this(filePath, DEFAULT_RECORD_DELIMITER, DEFAULT_FIELD_DELIMITER, tupleTypeInfo, includedFieldsMask);
	}

	public RFCTupleCsvInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, TupleTypeInfoBase<T> tupleTypeInfo, boolean[] includedFieldsMask) {
		super(filePath);
		configure(lineDelimiter, fieldDelimiter, tupleTypeInfo, includedFieldsMask);
	}

	private void configure(String lineDelimiter, char fieldDelimiter,
			TupleTypeInfoBase<T> tupleTypeInfo, boolean[] includedFieldsMask) {

		if (tupleTypeInfo.getArity() == 0) {
			throw new IllegalArgumentException("Tuple size must be greater than 0.");
		}

		if (includedFieldsMask == null) {
			includedFieldsMask = createDefaultMask(tupleTypeInfo.getArity());
		}

		tupleSerializer = (TupleSerializerBase<T>) tupleTypeInfo.createSerializer(new ExecutionConfig());

		setRecordDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);

		TypeInformation[] classes = new TypeInformation[tupleTypeInfo.getArity()];

		for (int i = 0; i < tupleTypeInfo.getArity(); i++) {
			classes[i] = tupleTypeInfo.getTypeAt(i);
		}
		this.tupleTypeInfo = tupleTypeInfo;
		setOnlySupportedFieldsTypes(includedFieldsMask, classes);
	}

	@Override
	public T fillRecord(T reuse, Object[] parsedValues) {
		return tupleSerializer.createOrReuseInstance(parsedValues, reuse);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return tupleTypeInfo;
	}
}
