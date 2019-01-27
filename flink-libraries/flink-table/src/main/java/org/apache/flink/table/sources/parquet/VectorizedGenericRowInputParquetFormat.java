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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to {@link Row}.
 */
public class VectorizedGenericRowInputParquetFormat extends ParquetInputFormat<GenericRow, GenericRow> implements ResultTypeQueryable<GenericRow> {

	private static final long serialVersionUID = -2569974518641072883L;
	private BaseRowSerializer<BaseRow> serializer;
	private transient GenericRow reuse;

	public VectorizedGenericRowInputParquetFormat(Path filePath, InternalType[] fieldTypes, String[] fieldNames) {
		super(filePath, fieldTypes, fieldNames);
		this.serializer = new BaseRowSerializer<>(fieldTypes);
	}

	@Override
	protected RecordReader createReader(FilterPredicate filter) {
		return new ParquetVectorizedGenericRowReader(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<GenericRow> getProducedType() {
		return (TypeInformation) new BaseRowTypeInfo(TypeConverters.createExternalTypeInfoFromDataTypes(fieldTypes));
	}

	@Override
	protected GenericRow convert(GenericRow current) {
		if (reuse == null) {
			return (GenericRow) serializer.copy(current);
		} else {
			for (int i = 0; i < current.getArity(); ++i) {
				reuse.update(i, current.getField(i));
			}
			return reuse;
		}
	}
}
