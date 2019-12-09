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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.normalizeTableSchema;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link TableSource} for JDBC.
 */
public class JDBCTableSource implements
		StreamTableSource<Row>,
		ProjectableTableSource<Row>,
		LookupableTableSource<Row> {

	private final JDBCOptions options;
	private final JDBCReadOptions readOptions;
	private final JDBCLookupOptions lookupOptions;
	private final TableSchema schema;

	// index of fields selected, null means that all fields are selected
	private final int[] selectFields;
	private final RowTypeInfo returnType;

	private JDBCTableSource(
		JDBCOptions options, JDBCReadOptions readOptions, JDBCLookupOptions lookupOptions, TableSchema schema) {
		this(options, readOptions, lookupOptions, schema, null);
	}

	private JDBCTableSource(
		JDBCOptions options, JDBCReadOptions readOptions, JDBCLookupOptions lookupOptions,
		TableSchema schema, int[] selectFields) {
		this.options = options;
		this.readOptions = readOptions;
		this.lookupOptions = lookupOptions;
		this.schema = schema;

		this.selectFields = selectFields;

		final TypeInformation<?>[] schemaTypeInfos = schema.getFieldTypes();
		final String[] schemaFieldNames = schema.getFieldNames();
		if (selectFields != null) {
			TypeInformation<?>[] typeInfos = new TypeInformation[selectFields.length];
			String[] typeNames = new String[selectFields.length];
			for (int i = 0; i < selectFields.length; i++) {
				typeInfos[i] = schemaTypeInfos[selectFields[i]];
				typeNames[i] = schemaFieldNames[selectFields[i]];
			}
			this.returnType = new RowTypeInfo(typeInfos, typeNames);
		} else {
			this.returnType = new RowTypeInfo(schemaTypeInfos, schemaFieldNames);
		}
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.createInput(getInputFormat(), getReturnType()).name(explainSource());
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return JDBCLookupFunction.builder()
				.setOptions(options)
				.setLookupOptions(lookupOptions)
				.setFieldTypes(returnType.getFieldTypes())
				.setFieldNames(returnType.getFieldNames())
				.setKeyNames(lookupKeys)
				.build();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return returnType;
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		return new JDBCTableSource(options, readOptions, lookupOptions, schema, fields);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	public static Builder builder() {
		return new Builder();
	}

	private JDBCInputFormat getInputFormat() {
		JDBCInputFormat.JDBCInputFormatBuilder builder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(options.getDriverName())
				.setDBUrl(options.getDbURL())
				.setUsername(options.getUsername())
				.setPassword(options.getPassword())
				.setRowTypeInfo(new RowTypeInfo(returnType.getFieldTypes(), returnType.getFieldNames()));

		if (readOptions.getFetchSize() != 0) {
			builder.setFetchSize(readOptions.getFetchSize());
		}

		final JDBCDialect dialect = options.getDialect();
		String query = dialect.getSelectFromStatement(
			options.getTableName(), returnType.getFieldNames(), new String[0]);
		if (readOptions.getPartitionColumnName().isPresent()) {
			long lowerBound = readOptions.getPartitionLowerBound().get();
			long upperBound = readOptions.getPartitionUpperBound().get();
			int numPartitions = readOptions.getNumPartitions().get();
			builder.setParametersProvider(
				new NumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions));
			query += " WHERE " +
				dialect.quoteIdentifier(readOptions.getPartitionColumnName().get()) +
				" BETWEEN ? AND ?";
		}
		builder.setQuery(query);

		return builder.finish();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JDBCTableSource) {
			JDBCTableSource source = (JDBCTableSource) o;
			return Objects.equals(options, source.options) &&
				Objects.equals(readOptions, source.readOptions) &&
				Objects.equals(lookupOptions, source.lookupOptions) &&
				Objects.equals(schema, source.schema) &&
				Arrays.equals(selectFields, source.selectFields);
		} else {
			return false;
		}
	}

	/**
	 * Builder for a {@link JDBCTableSource}.
	 */
	public static class Builder {

		private JDBCOptions options;
		private JDBCReadOptions readOptions;
		private JDBCLookupOptions lookupOptions;
		private TableSchema schema;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, scan related options.
		 * {@link JDBCReadOptions} will be only used for {@link StreamTableSource}.
		 */
		public Builder setReadOptions(JDBCReadOptions readOptions) {
			this.readOptions = readOptions;
			return this;
		}

		/**
		 * optional, lookup related options.
		 * {@link JDBCLookupOptions} only be used for {@link LookupableTableSource}.
		 */
		public Builder setLookupOptions(JDBCLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, table schema of this table source.
		 */
		public Builder setSchema(TableSchema schema) {
			this.schema = normalizeTableSchema(schema);
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCTableSource
		 */
		public JDBCTableSource build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(schema, "No schema supplied.");
			if (readOptions == null) {
				readOptions = JDBCReadOptions.builder().build();
			}
			if (lookupOptions == null) {
				lookupOptions = JDBCLookupOptions.builder().build();
			}
			return new JDBCTableSource(options, readOptions, lookupOptions, schema);
		}
	}
}
