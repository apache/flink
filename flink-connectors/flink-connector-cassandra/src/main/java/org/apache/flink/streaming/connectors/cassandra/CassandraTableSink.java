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
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * A Cassandra  {@link AppendStreamTableSink}.
 */
public class CassandraTableSink implements AppendStreamTableSink<Row> {

	private final ClusterBuilder builder;
	private final String cql;
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private final Properties properties;

	public CassandraTableSink(ClusterBuilder builder, String cql, Properties properties) {
		this.builder = Preconditions.checkNotNull(builder, "ClusterBuilder must not be null");
		this.cql = Preconditions.checkNotNull(cql, "Cql must not be null");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null");
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
	public CassandraTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		CassandraTableSink cassandraTableSink = new CassandraTableSink(this.builder, this.cql, this.properties);
		cassandraTableSink.fieldNames = Preconditions.checkNotNull(fieldNames, "FieldNames must not be null");
		cassandraTableSink.fieldTypes = Preconditions.checkNotNull(fieldTypes, "fieldTypes must not be null");
		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
			"Number of provided field names and types does not match.");
		return cassandraTableSink;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		try {
			CassandraSink.addSink(dataStream)
				.setClusterBuilder(this.builder)
				.setQuery(this.cql)
				.build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
