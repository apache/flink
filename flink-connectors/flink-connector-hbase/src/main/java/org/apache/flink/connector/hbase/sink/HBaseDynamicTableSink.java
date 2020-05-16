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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.hbase.options.HBaseOptions;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

/**
 * HBase table sink implementation.
 */
@Internal
public class HBaseDynamicTableSink implements DynamicTableSink {

	private final HBaseTableSchema hbaseTableSchema;
	private final HBaseOptions hbaseOptions;
	private final HBaseWriteOptions writeOptions;
	private final String nullStringLiteral;

	public HBaseDynamicTableSink(
			HBaseTableSchema hbaseTableSchema,
			HBaseOptions hbaseOptions,
			HBaseWriteOptions writeOptions,
			String nullStringLiteral) {
		this.hbaseTableSchema = hbaseTableSchema;
		this.hbaseOptions = hbaseOptions;
		this.writeOptions = writeOptions;
		this.nullStringLiteral = nullStringLiteral;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		Configuration hbaseClientConf = HBaseConfiguration.create();
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseOptions.getZkQuorum());
		hbaseOptions.getZkNodeParent().ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));
		HBaseSinkFunction<RowData> sinkFunction = new HBaseSinkFunction<>(
			hbaseOptions.getTableName(),
			hbaseClientConf,
			new RowDataToMutationConverter(hbaseTableSchema, nullStringLiteral),
			writeOptions.getBufferFlushMaxSizeInBytes(),
			writeOptions.getBufferFlushMaxRows(),
			writeOptions.getBufferFlushIntervalMillis());
		return SinkFunctionProvider.of(sinkFunction);
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		// UPSERT mode
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public DynamicTableSink copy() {
		return new HBaseDynamicTableSink(hbaseTableSchema, hbaseOptions, writeOptions, nullStringLiteral);
	}

	@Override
	public String asSummaryString() {
		return "HBase";
	}

	// -------------------------------------------------------------------------------------------

	@VisibleForTesting
	public HBaseTableSchema getHBaseTableSchema() {
		return this.hbaseTableSchema;
	}

	@VisibleForTesting
	public HBaseOptions getHBaseOptions() {
		return hbaseOptions;
	}

	@VisibleForTesting
	public HBaseWriteOptions getWriteOptions() {
		return writeOptions;
	}
}
