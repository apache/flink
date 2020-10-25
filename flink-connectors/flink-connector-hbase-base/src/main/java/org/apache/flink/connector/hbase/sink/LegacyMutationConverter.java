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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.util.HBaseReadWriteHelper;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * Legacy implementation for {@link org.apache.flink.connector.hbase.source.AbstractHBaseTableSource}.
 */
public class LegacyMutationConverter implements HBaseMutationConverter<Tuple2<Boolean, Row>> {
	private static final long serialVersionUID = 7358222494016900667L;

	private final HBaseTableSchema schema;

	private transient HBaseReadWriteHelper helper;

	public LegacyMutationConverter(HBaseTableSchema schema) {
		this.schema = schema;
	}

	@Override
	public void open() {
		this.helper = new HBaseReadWriteHelper(schema);
	}

	@Override
	public Mutation convertToMutation(Tuple2<Boolean, Row> record) {
		if (record.f0) {
			return helper.createPutMutation(record.f1);
		} else {
			return helper.createDeleteMutation(record.f1);
		}
	}
}
