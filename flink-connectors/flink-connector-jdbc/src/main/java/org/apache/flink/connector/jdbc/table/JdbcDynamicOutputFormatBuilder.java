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
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.BufferReduceStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
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
				ctx -> createSimpleRowDataExecutor(dmlOptions.getDialect(), sql, logicalTypes, ctx, rowDataTypeInformation),
				JdbcBatchingOutputFormat.RecordExtractor.identity());
		}
	}

	private static JdbcBatchStatementExecutor<RowData> createKeyedRowExecutor(
			JdbcDialect dialect,
			int[] pkFields,
			LogicalType[] pkTypes,
			String sql,
			LogicalType[] logicalTypes) {
		final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(pkTypes));
		final Function<RowData, RowData> keyExtractor = createRowKeyExtractor(logicalTypes, pkFields);
		return JdbcBatchStatementExecutor.keyed(
			sql,
			keyExtractor,
			(st, record) -> rowConverter.toExternal(keyExtractor.apply(record), st));
	}

	private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
			JdbcDmlOptions opt,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] fieldTypes) {
		checkArgument(opt.getKeyFields().isPresent());
		int[] pkFields = Arrays.stream(opt.getKeyFields().get()).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		final Function<RowData, RowData> valueTransform = ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity();

		JdbcBatchStatementExecutor<RowData> upsertExecutor = createUpsertRowExecutor(opt, ctx, rowDataTypeInfo, pkFields, pkTypes, fieldTypes, valueTransform);
		JdbcBatchStatementExecutor<RowData> deleteExecutor = createDeleteExecutor(opt, pkFields, pkTypes, fieldTypes);

		return new BufferReduceStatementExecutor(
			upsertExecutor,
			deleteExecutor,
			createRowKeyExtractor(fieldTypes, pkFields),
			valueTransform);
	}

	private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
			JdbcDmlOptions opt,
			RuntimeContext ctx,
			TypeInformation<RowData> rowDataTypeInfo,
			int[] pkFields,
			LogicalType[] pkTypes,
			LogicalType[] fieldTypes,
			Function<RowData, RowData> valueTransform) {
		checkArgument(opt.getKeyFields().isPresent());
		JdbcDialect dialect = opt.getDialect();
		return opt.getDialect()
			.getUpsertStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get())
			.map(sql -> createSimpleRowDataExecutor(dialect, sql, fieldTypes, ctx, rowDataTypeInfo))
			.orElseGet(() ->
				new InsertOrUpdateJdbcExecutor<>(
					opt.getDialect().getRowExistsStatement(opt.getTableName(), opt.getKeyFields().get()),
					opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
					opt.getDialect().getUpdateStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get()),
					createRowDataJdbcStatementBuilder(dialect, pkTypes),
					createRowDataJdbcStatementBuilder(dialect, fieldTypes),
					createRowDataJdbcStatementBuilder(dialect, fieldTypes),
					createRowKeyExtractor(fieldTypes, pkFields),
					valueTransform));
	}

	private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
			JdbcDmlOptions dmlOptions,
			int[] pkFields,
			LogicalType[] pkTypes,
			LogicalType[] fieldTypes) {
		checkArgument(dmlOptions.getKeyFields().isPresent());
		String[] pkNames = Arrays.stream(pkFields).mapToObj(k -> dmlOptions.getFieldNames()[k]).toArray(String[]::new);
		String deleteSql = dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), pkNames);
		return createKeyedRowExecutor(dmlOptions.getDialect(), pkFields, pkTypes, deleteSql, fieldTypes);
	}

	private static Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
		for (int i = 0; i < pkFields.length; i++) {
			fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
		}
		return row -> getPrimaryKey(row, fieldGetters);
	}

	private static JdbcBatchStatementExecutor<RowData> createSimpleRowDataExecutor(JdbcDialect dialect, String sql, LogicalType[] fieldTypes, RuntimeContext ctx, TypeInformation<RowData> rowDataTypeInfo) {
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		return JdbcBatchStatementExecutor.simple(
			sql,
			createRowDataJdbcStatementBuilder(dialect, fieldTypes),
			ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity());
	}

	/**
	 * Creates a {@link JdbcStatementBuilder} for {@link RowData} using the provided SQL types array.
	 * Uses {@link JdbcUtils#setRecordToStatement}
	 */
	private static JdbcStatementBuilder<RowData> createRowDataJdbcStatementBuilder(JdbcDialect dialect, LogicalType[] types) {
		final JdbcRowConverter converter = dialect.getRowConverter(RowType.of(types));
		return (st, record) -> converter.toExternal(record, st);
	}

	private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
		GenericRowData pkRow = new GenericRowData(fieldGetters.length);
		for (int i = 0; i < fieldGetters.length; i++) {
			pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
		}
		return pkRow;
	}
}
