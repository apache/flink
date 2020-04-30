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

package org.apache.flink.walkthrough.common.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * A table source for reading an unbounded set of transactions.
 *
 * <p>This table could be backed by a message queue or other streaming data source.
 */
@PublicEvolving
@SuppressWarnings({"deprecation", "unused"})
public class UnboundedTransactionTableSource
		implements StreamTableSource<Row>,
		DefinedRowtimeAttributes {

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv
			.addSource(new TransactionSource())
			.map(transactionRowMapFunction())
			.returns(getTableSchema().toRowType());
	}

	private MapFunction<Transaction, Row> transactionRowMapFunction() {
		return transaction -> Row.of(
			transaction.getAccountId(),
			new Timestamp(transaction.getTimestamp()),
			transaction.getAmount());
	}

	@Override
	public DataType getProducedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
			.field("accountId", Types.LONG)
			.field("timestamp", Types.SQL_TIMESTAMP)
			.field("amount", Types.DOUBLE)
			.build();
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return Collections.singletonList(
			new RowtimeAttributeDescriptor(
				"timestamp",
				new ExistingField("timestamp"),
				new BoundedOutOfOrderTimestamps(100)));
	}
}
