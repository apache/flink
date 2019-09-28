/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * An {@link AppendStreamTableSink} to write an append stream Table to a Cassandra table.
 */
public class CassandraAppendTableSink implements AppendStreamTableSink<Row> {

	private final ClusterBuilder builder;
	private final String cql;
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private final Properties properties;

	public CassandraAppendTableSink(ClusterBuilder builder, String cql) {
		this.builder = Preconditions.checkNotNull(builder, "ClusterBuilder must not be null.");
		this.cql = Preconditions.checkNotNull(cql, "CQL query must not be null.");
		this.properties = new Properties();
	}

	public CassandraAppendTableSink(ClusterBuilder builder, String cql, Properties properties) {
		this.builder = Preconditions.checkNotNull(builder, "ClusterBuilder must not be null.");
		this.cql = Preconditions.checkNotNull(cql, "CQL query must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes);
	}

	@Override
	public String[] getFieldNames() {
		return this.fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return this.fieldTypes;
	}

	@Override
	public CassandraAppendTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		CassandraAppendTableSink cassandraTableSink = new CassandraAppendTableSink(this.builder, this.cql, this.properties);
		cassandraTableSink.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names must not be null.");
		cassandraTableSink.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types must not be null.");
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");
		return cassandraTableSink;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		if (!(dataStream.getType() instanceof RowTypeInfo)) {
			throw new TableException("No support for the type of the given DataStream: " + dataStream.getType());
		}

		CassandraRowSink sink = new CassandraRowSink(
			dataStream.getType().getArity(),
			cql,
			builder,
			CassandraSinkBaseConfig.newBuilder().build(),
			new NoOpCassandraFailureHandler());

		return dataStream
				.addSink(sink)
				.setParallelism(dataStream.getParallelism())
				.name(TableConnectorUtils.generateRuntimeName(this.getClass(), fieldNames));

	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		consumeDataStream(dataStream);
	}
}
