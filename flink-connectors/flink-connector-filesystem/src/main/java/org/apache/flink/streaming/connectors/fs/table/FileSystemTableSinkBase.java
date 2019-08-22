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

package org.apache.flink.streaming.connectors.fs.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * a base file system table sink .
 */
@Internal
public abstract class FileSystemTableSinkBase implements AppendStreamTableSink<Row> {

	/**
	 * The schema of the table.
	 */
	protected final TableSchema schema;
	protected final StreamingFileSink<Row> sink;

	protected FileSystemTableSinkBase(TableSchema schema, StreamingFileSink sink) {
		this.schema = schema;
		this.sink = sink;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		getDataStream(dataStream).addSink(sink)
			.name(TableConnectorUtils.generateRuntimeName(
				this.getClass(),
				schema.getFieldNames()));
	}

	abstract DataStream getDataStream(DataStream dataStream);

	@Override
	public TypeInformation<Row> getOutputType() {
		return schema.toRowType();
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) ||
			!Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " +
				Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " +
				Arrays.toString(fieldTypes));
		}
		return this;
	}
}
