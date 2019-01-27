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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to Tuple.
 */
public class TupleParquetInputFormat<OUT> extends ParquetInputFormat<OUT, Row> {
	private static final long serialVersionUID = -9021324522796675550L;

	private final TupleSerializerBase<OUT> tupleSerializer;

	private transient OUT reuse;

	public TupleParquetInputFormat(Path filePath, TupleTypeInfoBase<OUT> tupleTypeInfo, String[] fieldNames) {
		this(filePath, tupleTypeInfo, fieldNames, new ExecutionConfig());
	}

	public TupleParquetInputFormat(Path filePath, TupleTypeInfoBase<OUT> tupleTypeInfo, String[] fieldNames,
			ExecutionConfig config) {
		super(filePath, extractTypeInfo(tupleTypeInfo), fieldNames);
		this.tupleSerializer = (TupleSerializerBase<OUT>) tupleTypeInfo.createSerializer(config);
	}

	@Override
	public OUT convert(Row current) {
		Object[] values = new Object[current.getArity()];
		for (int i = 0; i < current.getArity(); ++i) {
			values[i] = current.getField(i);
		}
		if (reuse == null) {
			return tupleSerializer.createInstance(values);
		} else {
			return tupleSerializer.createOrReuseInstance(values, reuse);
		}
	}

	private static <OUT> InternalType[] extractTypeInfo(TupleTypeInfoBase<OUT> tupleTypeInfo) {
		Preconditions.checkNotNull(tupleTypeInfo);
		return Arrays.stream(((RowType) TypeConverters.createInternalTypeFromTypeInfo(tupleTypeInfo)).getFieldTypes())
				.map(DataType::toInternalType).toArray(InternalType[]::new);
	}

}
