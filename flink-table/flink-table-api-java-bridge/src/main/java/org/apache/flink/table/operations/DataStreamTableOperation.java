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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;

import java.util.Collections;
import java.util.List;

/**
 * Describes a relational operation that reads from a {@link DataStream}.
 */
@Internal
public class DataStreamTableOperation<E> implements TableOperation {

	private final DataStream<E> dataStream;
	private final int[] fieldIndices;
	private final TableSchema tableSchema;

	public DataStreamTableOperation(
			DataStream<E> dataStream,
			int[] fieldIndices,
			TableSchema tableSchema) {
		this.dataStream = dataStream;
		this.tableSchema = tableSchema;
		this.fieldIndices = fieldIndices;
	}

	public DataStream<E> getDataStream() {
		return dataStream;
	}

	public int[] getFieldIndices() {
		return fieldIndices;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitOther(this);
	}
}
