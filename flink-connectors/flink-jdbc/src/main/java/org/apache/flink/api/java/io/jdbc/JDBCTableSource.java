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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link TableSource} for JDBC.
 * Now only support {@link LookupableTableSource}.
 */
public class JDBCTableSource implements LookupableTableSource<Row> {

	private final JDBCOptions options;
	private final JDBCLookupOptions lookupOptions;
	private final TableSchema schema;

	public JDBCTableSource(
			JDBCOptions options, JDBCLookupOptions lookupOptions, TableSchema schema) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return JDBCLookupFunction.builder()
				.setOptions(options)
				.setLookupOptions(lookupOptions)
				.setFieldTypes(schema.getFieldTypes())
				.setFieldNames(schema.getFieldNames())
				.setKeyNames(lookupKeys)
				.build();
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

	/**
	 * Builder for a {@link JDBCTableSource}.
	 */
	public static class Builder {

		private JDBCOptions options;
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
			this.schema = schema;
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
			return new JDBCTableSource(options, lookupOptions, schema);
		}
	}
}
