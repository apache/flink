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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory base for creating configured instances of {@link CsvTableSink} in a stream environment.
 */
@Experimental
public class PrintTableSinkFactory implements TableSinkFactory<Tuple2<Boolean, Row>> {

	public static final String CONNECTOR_TYPE_VALUE = "print";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);
		return properties;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> createTableSink(Context context) {
		return new RetractStreamTableSink<Row>() {

			@Override
			public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames,
					TypeInformation<?>[] fieldTypes) {
				return this;
			}

			@Override
			public TypeInformation<Row> getRecordType() {
				return context.getTable().getSchema().toRowType();
			}

			@Override
			public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
				return dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

					@Override
					public void invoke(Tuple2<Boolean, Row> value, Context context) {
						System.out.println(value);
					}
				}).setParallelism(dataStream.getParallelism());
			}
		};
	}
}
