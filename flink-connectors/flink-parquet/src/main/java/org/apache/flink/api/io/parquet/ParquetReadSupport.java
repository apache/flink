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

package org.apache.flink.api.io.parquet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * A Parquet {@link ReadSupport} implementation for reading Parquet records as {@link Row}.
 */
public class ParquetReadSupport extends ReadSupport<Row> {

	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;

	public ParquetReadSupport(TypeInformation<?>[] fieldTypes, String[] fieldNames) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	/**
	 * Called on executor side before {@link #prepareForRead(Configuration, Map, MessageType, ReadContext)} and
	 * instantiating actual Parquet record readers.
	 * Responsible for figuring out Parquet requested schema used for column pruning.
	 */
	@Override
	public ReadContext init(InitContext context) {
		MessageType requestedSchema = clipParquetSchema(context.getFileSchema().asGroupType(), fieldNames);
		return new ReadContext(requestedSchema, new HashMap<String, String>());
	}

	/**
	 * Called on executor side after {@link #init(InitContext)}, before instantiating actual Parquet record readers.
	 * Responsible for instantiating {@link RecordMaterializer}, which is used for converting Parquet  records to
	 * {@link Row}.
	 */
	@Override
	public RecordMaterializer<Row> prepareForRead(
			Configuration configuration,
			Map<String, String> keyValueMetaData,
			MessageType fileSchema,
			ReadContext readContext) {

		GroupType parquetRequestedSchema = readContext.getRequestedSchema();
		final ParquetRecordConverter recordConverter = new ParquetRecordConverter(parquetRequestedSchema, fieldTypes);

		return new RecordMaterializer<Row>() {

			@Override
			public Row getCurrentRecord() {
				return recordConverter.getCurrentRecord();
			}

			@Override
			public GroupConverter getRootConverter() {
				return recordConverter;
			}
		};
	}

	/**
	 * Clips `parquetSchema` according to `fieldNames`.
	 */
	private MessageType clipParquetSchema(GroupType parquetSchema, String[] fieldNames) {
		Type[] types = new Type[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			if (parquetSchema.getFieldIndex(fieldName) < 0) {
				throw new IllegalArgumentException(fieldName + " does not exist");
			}
			types[i] = parquetSchema.getType(fieldName);
		}
		return Types.buildMessage().addFields(types).named("flink-parquet");
	}

}
