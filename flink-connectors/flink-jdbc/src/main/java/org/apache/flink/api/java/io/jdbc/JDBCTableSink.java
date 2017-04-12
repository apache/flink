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
package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.UUID;

public class JDBCTableSink extends GenericWriteAheadSink<Row> implements StreamTableSink<Row> {
	private final JDBCOutputFormat outputFormat;
	private final CheckpointCommitter committer;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;

	public JDBCTableSink(CheckpointCommitter committer, TypeSerializer<Row> serializer,
				JDBCOutputFormat outputFormat, String[] fieldNames,
				TypeInformation[] fieldTypes) throws Exception {
		super(committer, serializer, UUID.randomUUID().toString().replace("-", "_"));
		this.outputFormat = outputFormat;
		this.committer = committer;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream.transform("JDBC Sink", getOutputType(), this);
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		try {
			return new JDBCTableSink(committer, serializer, outputFormat, fieldNames, fieldTypes);
		} catch (Exception e) {
			LOG.warn("Failed to create a copy of the sink.", e);
			return null;
		}
	}

	@Override
	protected boolean sendValues(Iterable<Row> value, long timestamp) throws Exception {
		for (Row r : value) {
			try {
				outputFormat.writeRecord(r);
			} catch (IOException e) {
				LOG.warn("Sending a value failed.", e);
				return false;
			}
		}
		return true;
	}
}
