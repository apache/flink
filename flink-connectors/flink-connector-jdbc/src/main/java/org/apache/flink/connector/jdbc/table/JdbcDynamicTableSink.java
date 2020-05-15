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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicOutputFormat.DynamicOutputFormatBuilder;

/**
 * A {@link DynamicTableSink} for JDBC.
 */
@Internal
public class JdbcDynamicTableSink implements DynamicTableSink {

	private static final String name = "JdbcTableSink";
	private final JdbcOptions jdbcOptions;
	private final JdbcExecutionOptions executionOptions;
	private final JdbcDmlOptions dmlOptions;
	private final DataType rowDataType;
	private final DataType[] fieldTypes;

	public JdbcDynamicTableSink(JdbcOptions jdbcOptions, JdbcExecutionOptions executionOptions, JdbcDmlOptions dmlOptions, DataType rowDataType, DataType[] fieldTypes) {
		this.jdbcOptions = jdbcOptions;
		this.executionOptions = executionOptions;
		this.dmlOptions = dmlOptions;
		this.rowDataType = rowDataType;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		final TypeInformation<RowData> rowDataTypeInformation = (TypeInformation<RowData>) context.createTypeInformation(rowDataType);
		final DynamicOutputFormatBuilder builder = JdbcDynamicOutputFormat.dynamicOutputFormatBuilder();

		builder.setJdbcOptions(jdbcOptions);
		builder.setJdbcDmlOptions(dmlOptions);
		builder.setJdbcExecutionOptions(executionOptions);
		builder.setRowDataTypeInfo(rowDataTypeInformation);
		builder.setFieldDataTypes(fieldTypes);
		return OutputFormatProvider.of(builder.build());
	}

	@Override
	public DynamicTableSink copy() {
		return new JdbcDynamicTableSink(jdbcOptions, executionOptions, dmlOptions, rowDataType, fieldTypes);
	}

	@Override
	public String asSummaryString() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof JdbcDynamicTableSink)) {
			return false;
		}
		JdbcDynamicTableSink that = (JdbcDynamicTableSink) o;
		return Objects.equals(jdbcOptions, that.jdbcOptions) &&
			Objects.equals(executionOptions, that.executionOptions) &&
			Objects.equals(dmlOptions, that.dmlOptions) &&
			Objects.equals(rowDataType, that.rowDataType) &&
			Arrays.equals(fieldTypes, that.fieldTypes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(jdbcOptions, executionOptions, dmlOptions, rowDataType);
		result = 31 * result + Arrays.hashCode(fieldTypes);
		return result;
	}
}
