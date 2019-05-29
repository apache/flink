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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.formats.thrift.typeutils.ThriftSerializer;
import org.apache.flink.formats.thrift.utils.ThriftWriter;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Table sink for storing data in thrift format.
 */
public class ThriftTableSink extends TableSinkBase<Row> implements BatchTableSink<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializer.class);

	private Class<? extends TProtocol> tProtocolClass;
	private String path;
	private ThriftWriter thriftWriter;
	private ThriftRowSerializationSchema rowSerializationSchema;

	public ThriftTableSink(ThriftRowSerializationSchema rowSerializationSchema, String path,
		Class<? extends TProtocol> tProtocolClass) {
		this.rowSerializationSchema = rowSerializationSchema;
		this.path = path;
		this.tProtocolClass = tProtocolClass;
		this.thriftWriter = new ThriftWriter(new File(path), tProtocolClass);
	}

	public void emitDataSet(DataSet<Row> dataSet) {
		try {
			thriftWriter.open();
			List<Row> records = dataSet.collect();
			for (Row record: records) {
				byte[] bytes = rowSerializationSchema.serialize(record);
				thriftWriter.write(bytes);
			}
		} catch (Exception e) {
			LOG.error("Failed to emit data", e);
		} finally {
			try {
				thriftWriter.close();
			} catch (Exception e) {

			}
		}
	}

	public ThriftTableSink copy() {
		return new ThriftTableSink(rowSerializationSchema, path, tProtocolClass);
	}

	public TypeInformation<Row> getOutputType() {
		return TypeInformation.of(Row.class);
	}
}
