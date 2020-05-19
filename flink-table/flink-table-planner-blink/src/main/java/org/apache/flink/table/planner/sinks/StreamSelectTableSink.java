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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * A {@link RetractStreamTableSink} for streaming select job to collect the result to local.
 *
 * <p>{@link RowData} contains {@link RowKind} attribute which
 * can represents all kind of changes. The boolean flag is useless here,
 * only because {@link RetractStreamTableSink} requires Tuple2&lt;Boolean, T&gt; type.
 */
public class StreamSelectTableSink
		extends SelectTableSinkBase<Tuple2<Boolean, RowData>>
		implements RetractStreamTableSink<RowData> {

	public StreamSelectTableSink(TableSchema tableSchema) {
		super(tableSchema, new TupleTypeInfo<Tuple2<Boolean, RowData>>(
				Types.BOOLEAN,
				createRowDataTypeInfo(tableSchema)).createSerializer(new ExecutionConfig()));
	}

	@Override
	public TypeInformation<RowData> getRecordType() {
		return createRowDataTypeInfo(getTableSchema());
	}

	@Override
	protected Row convertToRow(Tuple2<Boolean, RowData> tuple2) {
		// convert Tuple2<Boolean, RowData> to Row
		return converter.toExternal(tuple2.f1);
	}
}
