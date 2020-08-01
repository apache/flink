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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.parquet.schema.MessageType;

/**
 * An implementation of {@link ParquetInputFormat} to read {@link Row} records from Parquet files.
 */
public class ParquetRowInputFormat extends ParquetInputFormat<Row> implements ResultTypeQueryable<Row> {
	private static final long serialVersionUID = 11L;

	public ParquetRowInputFormat(Path path, MessageType messageType) {
		super(path, messageType);
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	@Override
	protected Row convert(Row row) {
		return row;
	}
}
