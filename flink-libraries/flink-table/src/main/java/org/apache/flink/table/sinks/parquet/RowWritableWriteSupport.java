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

package org.apache.flink.table.sinks.parquet;

import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;

/**
 * RowWritableWriteSupport is a WriteSupport for the RowWritableWriter.
 */
public class RowWritableWriteSupport extends WriteSupport<BaseRow> {

	public static final String PARQUET_FLINK_SCHEMA = "parquet.flink.schema";

	private RowWritableWriter writer;
	private MessageType schema;
	private InternalType[] fieldTypes;

	public RowWritableWriteSupport(InternalType[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	public static void setSchema(final MessageType schema, final Configuration configuration) {
		configuration.set(PARQUET_FLINK_SCHEMA, schema.toString());
	}

	public static MessageType getSchema(final Configuration configuration) {
		return MessageTypeParser.parseMessageType(configuration.get(PARQUET_FLINK_SCHEMA));
	}

	@Override
	public WriteContext init(final Configuration configuration) {
		schema = getSchema(configuration);
		return new WriteContext(schema, new HashMap<String, String>());
	}

	@Override
	public void prepareForWrite(final RecordConsumer recordConsumer) {
		writer = new RowWritableWriter(fieldTypes, recordConsumer, schema);
	}

	@Override
	public void write(final BaseRow record) {
		writer.write(record);
	}
}

