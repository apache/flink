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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A HBase TableSource for stream table.
 */
public class HBaseStreamTableSource implements StreamTableSource<Row>, LookupableTableSource<Row> {

	private final HBaseTableContext hBaseTableContext;

	public HBaseStreamTableSource(HBaseTableContext hBaseTableContext) {
		this.hBaseTableContext = hBaseTableContext;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		Preconditions.checkArgument(null != lookupKeys && lookupKeys.length == 1,
			"HBase table can only be retrieved by rowKey for now.");
		try {
			return new HBaseLookupFunction(this.hBaseTableContext);
		} catch (IOException e) {
			throw new RuntimeException("encounter an IOException when initialize the HBase143LookupFunction.", e);
		}
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return this.hBaseTableContext.getTableSchema();
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public DataType getProducedDataType() {
		return hBaseTableContext.getTableSchema().toRowDataType();
	}

	@VisibleForTesting
	public HBaseTableContext gethBaseTableContext() {
		return hBaseTableContext;
	}
}
