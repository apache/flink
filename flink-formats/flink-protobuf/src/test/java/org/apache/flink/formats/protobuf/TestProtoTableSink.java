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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Test protobuf based sink.
 */
class TestProtoTableSink implements AppendStreamTableSink<Row>, Serializable {

	private final Class<? extends Message> protobufClass;
	private ProtobufRowSerializationSchema protobufRowSerializationSchema;
	private RowTypeInfo rowSchema;
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	public TestProtoTableSink(Class<? extends Message> protobufClass) {
		this.protobufClass = protobufClass;
	}

	// must be static
	public static final List<byte[]> MESSAGE_BYTES = new ArrayList<>();

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream.map(
			r -> {
				byte[] bytes = protobufRowSerializationSchema.serialize(r);
				MESSAGE_BYTES.add(bytes);
				return null;
			});
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return rowSchema;
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

		TestProtoTableSink copy = new TestProtoTableSink(this.protobufClass);

		copy.fieldNames = Preconditions.checkNotNull(fieldNames, "fieldNames");
		copy.fieldTypes = Preconditions.checkNotNull(fieldTypes, "fieldTypes");
		Preconditions.checkArgument(
			fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");
		copy.rowSchema = new RowTypeInfo(fieldTypes, fieldNames);

		copy.protobufRowSerializationSchema =
			new ProtobufRowSerializationSchema(protobufClass, copy.rowSchema);
		return copy;
	}

	/**
	 * Test sinks need to be static variables, so this must be called before sink reuse.
	 */
	public static void clear() {
		MESSAGE_BYTES.clear();
	}
}
