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

package org.apache.flink.table.factories.datagen;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * A {@link StreamTableSource} that emits each number from a given interval exactly once,
 * possibly in parallel. See {@link StatefulSequenceSource}.
 */
@Internal
public class DataGenTableSource implements ScanTableSource {

	private final DataGenerator[] fieldGenerators;
	private final TableSchema schema;
	private final long rowsPerSecond;
	private final Long numberOfRows;

	public DataGenTableSource(DataGenerator[] fieldGenerators, TableSchema schema, long rowsPerSecond, Long numberOfRows) {
		this.fieldGenerators = fieldGenerators;
		this.schema = schema;
		this.rowsPerSecond = rowsPerSecond;
		this.numberOfRows = numberOfRows;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
		boolean isBounded = numberOfRows == null;
		return SourceFunctionProvider.of(createSource(), isBounded);
	}

	@VisibleForTesting
	public DataGeneratorSource<RowData> createSource() {
		return new DataGeneratorSource<>(
			new RowGenerator(fieldGenerators, schema.getFieldNames()),
			rowsPerSecond, numberOfRows);
	}

	@Override
	public DynamicTableSource copy() {
		return new DataGenTableSource(fieldGenerators, schema, rowsPerSecond, numberOfRows);
	}

	@Override
	public String asSummaryString() {
		return "DataGenTableSource";
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	private static class RowGenerator implements DataGenerator<RowData> {

		private static final long serialVersionUID = 1L;

		private final DataGenerator[] fieldGenerators;
		private final String[] fieldNames;

		private RowGenerator(DataGenerator[] fieldGenerators, String[] fieldNames) {
			this.fieldGenerators = fieldGenerators;
			this.fieldNames = fieldNames;
		}

		@Override
		public void open(
			String name,
			FunctionInitializationContext context,
			RuntimeContext runtimeContext) throws Exception {
			for (int i = 0; i < fieldGenerators.length; i++) {
				fieldGenerators[i].open(fieldNames[i], context, runtimeContext);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			for (DataGenerator generator : fieldGenerators) {
				generator.snapshotState(context);
			}
		}

		@Override
		public boolean hasNext() {
			for (DataGenerator generator : fieldGenerators) {
				if (!generator.hasNext()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public RowData next() {
			GenericRowData row = new GenericRowData(fieldNames.length);
			for (int i = 0; i < fieldGenerators.length; i++) {
				row.setField(i, fieldGenerators[i].next());
			}
			return row;
		}
	}
}

