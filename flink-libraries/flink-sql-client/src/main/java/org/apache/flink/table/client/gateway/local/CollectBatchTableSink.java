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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Table sink for collecting the results locally all at once using accumulators.
 */
public class CollectBatchTableSink implements BatchTableSink<Row> {

	private final String accumulatorName;
	private final TypeSerializer<Row> serializer;

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	public CollectBatchTableSink(String accumulatorName, TypeSerializer<Row> serializer) {
		this.accumulatorName = accumulatorName;
		this.serializer = serializer;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return Types.ROW_NAMED(fieldNames, fieldTypes);
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
		final CollectBatchTableSink copy = new CollectBatchTableSink(accumulatorName, serializer);
		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet
			.output(new Utils.CollectHelper<>(accumulatorName, serializer))
			.name("SQL Client Batch Collect Sink");
	}

	/**
	 * Returns the serializer for deserializing the collected result.
	 */
	public TypeSerializer<Row> getSerializer() {
		return serializer;
	}
}
