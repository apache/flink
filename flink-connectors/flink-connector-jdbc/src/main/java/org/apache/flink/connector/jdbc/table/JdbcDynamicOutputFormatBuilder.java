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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableInsertOrUpdateStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for {@link JdbcBatchingOutputFormat} for Table/SQL.
 */
public class JdbcDynamicOutputFormatBuilder implements Serializable {

	private static final long serialVersionUID = 1L;

	private JdbcOptions jdbcOptions;
	private JdbcExecutionOptions executionOptions;
	private JdbcDmlOptions dmlOptions;
	private TypeInformation<RowData> rowDataTypeInformation;
	private DataType[] fieldDataTypes;

	public JdbcDynamicOutputFormatBuilder() {
	}

	public JdbcDynamicOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setJdbcExecutionOptions(JdbcExecutionOptions executionOptions) {
		this.executionOptions = executionOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
		this.dmlOptions = dmlOptions;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
		this.rowDataTypeInformation = rowDataTypeInfo;
		return this;
	}

	public JdbcDynamicOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
		this.fieldDataTypes = fieldDataTypes;
		return this;
	}

	public JdbcBatchingOutputFormat<RowData, ?, ?> build() {
		checkNotNull(jdbcOptions, "jdbc options can not be null");
		checkNotNull(dmlOptions, "jdbc dml options can not be null");
		checkNotNull(executionOptions, "jdbc execution options can not be null");

		final LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
			.map(DataType::getLogicalType)
			.toArray(LogicalType[]::new);
		if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
			//upsert query
			return new JdbcBatchingOutputFormat<>(
				new SimpleJdbcConnectionProvider(jdbcOptions),
				executionOptions,
				ctx -> createBufferReduceExecutor(dmlOptions, ctx, rowDataTypeInformation, logicalTypes),
				JdbcBatchingOutputFormat.RecordExtractor.identity());
		} else {
			// append only query
			final String sql = dmlOptions
				.getDialect()
				.getInsertIntoStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
			return new JdbcBatchingOutputFormat<>(
				new SimpleJdbcConnectionProvider(jdbcOptions),
				executionOptions,
				ctx -> createSimpleBufferedExecutor(
					ctx,
					dmlOptions.getDialect(),
					dmlOptions.getFieldNames(),
					logicalTypes,
					sql,
					rowDataTypeInformation),
				JdbcBatchingOutputFormat.RecordExtractor.identity());
		}
	}

	private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
			JdbcDmlOptions opt,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] fieldTypes) {
		checkArgument(opt.getKeyFields().isPresent());
		JdbcDialect dialect = opt.getDialect();
		String tableName = opt.getTableName();
		String[] pkNames = opt.getKeyFields().get();
		int[] pkFields = Arrays.stream(pkNames).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		final Function<RowData, RowData> valueTransform = ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity();

		return new TableBufferReducedStatementExecutor(
			createUpsertRowExecutor(dialect, tableName, opt.getFieldNames(), fieldTypes, pkFields, pkNames, pkTypes),
			createDeleteExecutor(dialect, tableName, pkNames, pkTypes),
			createRowKeyExtractor(fieldTypes, pkFields),
			valueTransform);
	}

	private static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
			RuntimeContext ctx,
			JdbcDialect dialect,
			String[] fieldNames,
			LogicalType[] fieldTypes,
			String sql,
			TypeInformation<RowData> rowDataTypeInfo) {
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		return new TableBufferedStatementExecutor(
			createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql),
			ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity()
		);
	}

	private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
			JdbcDialect dialect,
			String tableName,
			String[] fieldNames,
			LogicalType[] fieldTypes,
			int[] pkFields,
			String[] pkNames,
			LogicalType[] pkTypes) {
		return dialect
			.getUpsertStatement(tableName, fieldNames, pkNames)
			.map(sql -> createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql))
			.orElseGet(() -> createInsertOrUpdateExecutor(dialect, tableName, fieldNames, fieldTypes, pkFields, pkNames, pkTypes));
	}

	private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
			JdbcDialect dialect,
			String tableName,
			String[] pkNames,
			LogicalType[] pkTypes) {
		String deleteSql = dialect.getDeleteStatement(tableName, pkNames);
		return createSimpleRowExecutor(dialect, pkNames, pkTypes, deleteSql);
	}

	private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
			JdbcDialect dialect,
			String[] fieldNames,
			LogicalType[] fieldTypes,
			final String sql) {
		final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
		return new TableSimpleStatementExecutor(
			connection -> FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
			rowConverter
		);
	}

	private static JdbcBatchStatementExecutor<RowData> createInsertOrUpdateExecutor(
			JdbcDialect dialect,
			String tableName,
			String[] fieldNames,
			LogicalType[] fieldTypes,
			int[] pkFields,
			String[] pkNames,
			LogicalType[] pkTypes) {
		final String existStmt = dialect.getRowExistsStatement(tableName, pkNames);
		final String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
		final String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, pkNames);
		return new TableInsertOrUpdateStatementExecutor(
			connection -> FieldNamedPreparedStatement.prepareStatement(connection, existStmt, pkNames),
			connection -> FieldNamedPreparedStatement.prepareStatement(connection, insertStmt, fieldNames),
			connection -> FieldNamedPreparedStatement.prepareStatement(connection, updateStmt, fieldNames),
			dialect.getRowConverter(RowType.of(pkTypes)),
			dialect.getRowConverter(RowType.of(fieldTypes)),
			dialect.getRowConverter(RowType.of(fieldTypes)),
			createRowKeyExtractor(fieldTypes, pkFields)
		);
	}

	private static Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
		for (int i = 0; i < pkFields.length; i++) {
			fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
		}
		return row -> getPrimaryKey(row, fieldGetters);
	}

	private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
		GenericRowData pkRow = new GenericRowData(fieldGetters.length);
		for (int i = 0; i < fieldGetters.length; i++) {
			pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
		}
		return pkRow;
	}
}
