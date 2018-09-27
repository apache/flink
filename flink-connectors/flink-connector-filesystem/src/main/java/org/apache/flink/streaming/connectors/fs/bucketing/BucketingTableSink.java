/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * An bucketing Table sink.
 */
public class BucketingTableSink implements AppendStreamTableSink<Row> {

	private final BucketingSink<Row> sink;
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	/**
	 * Creates a new {@code BucketingTableSink} that writes table rows to the given base directory.
	 *
	 * @param sink The BucketingSink to which to write the table rows.
	 */
	public BucketingTableSink(BucketingSink<Row> sink) {
		this.sink = sink;
	}

	/**
	 * A builder to configure and build the BucketingTableSink.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 */
	public static BucketingTableSinkBuilder builder(String basePath) {
		return new BucketingTableSinkBuilder(basePath);
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream.addSink(sink).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
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
		BucketingTableSink copy;
		try {
			copy = new BucketingTableSink(InstantiationUtil.clone(sink));
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}
}
