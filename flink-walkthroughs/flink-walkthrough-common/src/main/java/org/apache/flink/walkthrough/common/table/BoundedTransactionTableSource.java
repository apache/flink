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

package org.apache.flink.walkthrough.common.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.walkthrough.common.source.TransactionRowInputFormat;

/**
 * A table source for reading a bounded set of transaction events.
 *
 * <p>This could be backed by a table, database, or other static data set.
 */
@PublicEvolving
@SuppressWarnings({"deprecation", "unused"})
public class BoundedTransactionTableSource extends InputFormatTableSource<Row> {
	@Override
	public InputFormat<Row, ?> getInputFormat() {
		return new TransactionRowInputFormat();
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
}
