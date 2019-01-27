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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;

/**
 * A subclass of {@link ParquetInputFormat} to read from
 * Parquet files and convert to {@link ColumnarRow}.
 */
public class VectorizedColumnRowInputParquetFormat extends ParquetInputFormat<ColumnarRow, ColumnarRow>
		implements ResultTypeQueryable<ColumnarRow> {

	private static final long serialVersionUID = -2569974518641072883L;

	private final long limit;

	public VectorizedColumnRowInputParquetFormat(Path filePath, InternalType[] fieldTypes,
			String[] fieldNames, long limit) {
		super(filePath, fieldTypes, fieldNames);
		this.limit = limit;
	}

	@Override
	protected RecordReader createReader(FilterPredicate filter) {
		return new ParquetVectorizedColumnRowReader(fieldTypes, fieldNames, limit);
	}

	@Override
	public TypeInformation<ColumnarRow> getProducedType() {
		return (TypeInformation) new BaseRowTypeInfo(TypeConverters.createExternalTypeInfoFromDataTypes(fieldTypes));
	}

	@Override
	protected ColumnarRow convert(ColumnarRow current) {
		return current;
	}
}
