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

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to {@link VectorizedColumnBatch}.
 */
public class VectorizedBatchParquetInputFormat extends ParquetInputFormat<VectorizedColumnBatch, VectorizedColumnBatch>
	implements ResultTypeQueryable<VectorizedColumnBatch> {

	private static final long serialVersionUID = -2569974518641272330L;

	public VectorizedBatchParquetInputFormat(Path filePath, InternalType[] fieldTypes, String[] fieldNames) {
		super(filePath, fieldTypes, fieldNames);
	}

	@Override
	protected VectorizedColumnBatch convert(
		VectorizedColumnBatch current) {
		return current;
	}

	@Override
	public VectorizedColumnBatchTypeInfo getProducedType() {
		return new VectorizedColumnBatchTypeInfo(fieldNames, TypeConverters.createExternalTypeInfoFromDataTypes(fieldTypes));
	}

	@Override
	protected RecordReader createReader(FilterPredicate filter) {
		return new ParquetVectorizedReader(fieldTypes, fieldNames);
	}
}
