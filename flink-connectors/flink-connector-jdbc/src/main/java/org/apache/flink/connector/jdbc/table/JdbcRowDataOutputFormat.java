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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * OutputFormat for {@link JdbcDynamicTableSource}.
 */
@Internal
public class JdbcRowDataOutputFormat extends JdbcBatchingOutputFormat<RowData, RowData, JdbcBatchStatementExecutor<RowData>> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataOutputFormat.class);

	private JdbcBatchStatementExecutor<RowData> deleteExecutor;
	private final JdbcDmlOptions dmlOptions;
	private final LogicalType[] logicalTypes;

	private JdbcRowDataOutputFormat(
			JdbcConnectionProvider connectionProvider,
			JdbcDmlOptions dmlOptions,
			JdbcExecutionOptions batchOptions,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] logicalTypes) {
		super(
			connectionProvider,
			batchOptions,
			ctx -> createUpsertRowExecutor(dmlOptions, ctx, rowDataTypeInfo, logicalTypes),
			RecordExtractor.identity());
		this.dmlOptions = dmlOptions;
		this.logicalTypes = logicalTypes;
	}

	private JdbcRowDataOutputFormat(
			JdbcConnectionProvider connectionProvider,
			JdbcDmlOptions dmlOptions,
			JdbcExecutionOptions batchOptions,
			TypeInformation<RowData> rowDataTypeInfo,
			LogicalType[] logicalTypes,
			String sql) {
		super(connectionProvider,
			batchOptions,
			ctx -> createSimpleRowDataExecutor(dmlOptions.getDialect(), sql, logicalTypes, ctx, rowDataTypeInfo),
			RecordExtractor.identity());
		this.dmlOptions = dmlOptions;
		this.logicalTypes = logicalTypes;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		deleteExecutor = createDeleteExecutor();
		super.open(taskNumber, numTasks);
		try {
			deleteExecutor.prepareStatements(connection);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private JdbcBatchStatementExecutor<RowData> createDeleteExecutor() {
		int[] pkFields = Arrays.stream(dmlOptions.getFieldNames()).mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> logicalTypes[f]).toArray(LogicalType[]::new);
		String deleteSql = dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
		return createKeyedRowExecutor(dmlOptions.getDialect(), pkFields, pkTypes, deleteSql, logicalTypes);
	}

	@Override
	protected void addToBatch(RowData original, RowData extracted) throws SQLException {
		switch (original.getRowKind()) {
			case INSERT:
			case UPDATE_AFTER:
				super.addToBatch(original, extracted);
				break;
			case DELETE:
			case UPDATE_BEFORE:
				deleteExecutor.addToBatch(extracted);
				break;
			default:
				throw new UnsupportedOperationException(
					String.format("unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER," +
						" DELETE, but get: %s.", original.getRowKind()));
		}
	}

	@Override
	public synchronized void close() {
		try {
			super.close();
		} finally {
			try {
				if (deleteExecutor != null) {
					deleteExecutor.closeStatements();
				}
			} catch (SQLException e) {
				LOG.warn("unable to close delete statement runner", e);
			}
		}
	}

	@Override
	protected void attemptFlush() throws SQLException {
		super.attemptFlush();
		deleteExecutor.executeBatch();
	}

	private static JdbcBatchStatementExecutor<RowData> createKeyedRowExecutor(JdbcDialect dialect, int[] pkFields, LogicalType[] pkTypes, String sql, LogicalType[] logicalTypes) {
		final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(logicalTypes));
		final Function<RowData, RowData>  keyExtractor = createRowKeyExtractor(logicalTypes, pkFields);
		return JdbcBatchStatementExecutor.keyed(
			sql,
			keyExtractor,
			(st, record) -> rowConverter
				.toExternal(keyExtractor.apply(record), st));
	}

	private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(JdbcDmlOptions opt, RuntimeContext ctx, TypeInformation<RowData> rowDataTypeInfo, LogicalType[] logicalTypes) {
		checkArgument(opt.getKeyFields().isPresent());

		int[] pkFields = Arrays.stream(opt.getKeyFields().get()).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		LogicalType[] pkTypes = Arrays.stream(pkFields).mapToObj(f -> logicalTypes[f]).toArray(LogicalType[]::new);
		JdbcDialect dialect = opt.getDialect();
		final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
		return opt.getDialect()
			.getUpsertStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get())
			.map(sql -> createSimpleRowDataExecutor(dialect, sql, logicalTypes, ctx, rowDataTypeInfo))
			.orElseGet(() ->
				new InsertOrUpdateJdbcExecutor<>(
					opt.getDialect().getRowExistsStatement(opt.getTableName(), opt.getKeyFields().get()),
					opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
					opt.getDialect().getUpdateStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get()),
					createRowDataJdbcStatementBuilder(dialect, pkTypes),
					createRowDataJdbcStatementBuilder(dialect, logicalTypes),
					createRowDataJdbcStatementBuilder(dialect, logicalTypes),
					createRowKeyExtractor(logicalTypes, pkFields),
					ctx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : r -> r));
	}

	private static Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
		final FieldGetter[] fieldGetters = new FieldGetter[pkFields.length];
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
	 * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided SQL types array.
	 * Uses {@link JdbcUtils#setRecordToStatement}
	 */
	private static JdbcStatementBuilder<RowData> createRowDataJdbcStatementBuilder(JdbcDialect dialect, LogicalType[] types) {
		final JdbcRowConverter converter = dialect.getRowConverter(RowType.of(types));
		return (st, record) -> converter.toExternal(record, st);
	}

	private static RowData getPrimaryKey(RowData row, FieldGetter[] fieldGetters) {
		GenericRowData pkRow = new GenericRowData(fieldGetters.length);
		for (int i = 0; i < fieldGetters.length; i++) {
			pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
		}
		return pkRow;
	}

	public static DynamicOutputFormatBuilder dynamicOutputFormatBuilder() {
		return new DynamicOutputFormatBuilder();
	}

	/**
	 * Builder for {@link JdbcRowDataOutputFormat}.
	 */
	public static class DynamicOutputFormatBuilder {
		private JdbcOptions jdbcOptions;
		private JdbcExecutionOptions executionOptions;
		private JdbcDmlOptions dmlOptions;
		private TypeInformation<RowData> rowDataTypeInformation;
		private DataType[] fieldDataTypes;

		private DynamicOutputFormatBuilder() {
		}

		public DynamicOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
			this.jdbcOptions = jdbcOptions;
			return this;
		}

		public DynamicOutputFormatBuilder setJdbcExecutionOptions(JdbcExecutionOptions executionOptions) {
			this.executionOptions = executionOptions;
			return this;
		}

		public DynamicOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
			this.dmlOptions = dmlOptions;
			return this;
		}

		public DynamicOutputFormatBuilder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
			this.rowDataTypeInformation = rowDataTypeInfo;
			return this;
		}

		public DynamicOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
			this.fieldDataTypes = fieldDataTypes;
			return this;
		}

		public JdbcRowDataOutputFormat build() {
			checkNotNull(jdbcOptions, "jdbc options can not be null");
			checkNotNull(dmlOptions, "jdbc dml options can not be null");
			checkNotNull(executionOptions, "jdbc execution options can not be null");

			final LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
			if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
				//upsert query
				return new JdbcRowDataOutputFormat(
					new SimpleJdbcConnectionProvider(jdbcOptions),
					dmlOptions,
					executionOptions,
					rowDataTypeInformation,
					logicalTypes);
			} else {
				// append only query
				final String sql = dmlOptions
					.getDialect()
					.getInsertIntoStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
				return new JdbcRowDataOutputFormat(
					new SimpleJdbcConnectionProvider(jdbcOptions),
					dmlOptions,
					executionOptions,
					rowDataTypeInformation,
					logicalTypes,
					sql);
			}
		}
	}
}

