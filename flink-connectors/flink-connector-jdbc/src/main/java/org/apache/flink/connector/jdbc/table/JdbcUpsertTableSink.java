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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An upsert {@link UpsertStreamTableSink} for JDBC.
 */
public class JdbcUpsertTableSink implements UpsertStreamTableSink<Row> {

	private final TableSchema schema;
	private final JdbcOptions options;
	private final int flushMaxSize;
	private final long flushIntervalMills;
	private final int maxRetryTime;

	private String[] keyFields;
	private boolean isAppendOnly;

	private JdbcUpsertTableSink(
			TableSchema schema,
			JdbcOptions options,
			int flushMaxSize,
			long flushIntervalMills,
			int maxRetryTime) {
		this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
		this.options = options;
		this.flushMaxSize = flushMaxSize;
		this.flushIntervalMills = flushIntervalMills;
		this.maxRetryTime = maxRetryTime;
	}

	private JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> newFormat() {
		if (!isAppendOnly && (keyFields == null || keyFields.length == 0)) {
			throw new UnsupportedOperationException("JdbcUpsertTableSink can not support ");
		}

		// sql types
		int[] jdbcSqlTypes = Arrays.stream(schema.getFieldTypes())
				.mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();

		return JdbcBatchingOutputFormat.builder()
			.setOptions(options)
			.setFieldNames(schema.getFieldNames())
			.setFlushMaxSize(flushMaxSize)
			.setFlushIntervalMills(flushIntervalMills)
			.setMaxRetryTimes(maxRetryTime)
			.setFieldTypes(jdbcSqlTypes)
			.setKeyFields(keyFields)
			.build();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		return dataStream
				.addSink(new GenericJdbcSinkFunction<>(newFormat()))
				.setParallelism(dataStream.getParallelism())
				.name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
	}

	@Override
	public void setKeyFields(String[] keys) {
		this.keyFields = keys;
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		this.isAppendOnly = isAppendOnly;
	}

	@Override
	public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
					"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
					"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}

		JdbcUpsertTableSink copy = new JdbcUpsertTableSink(schema, options, flushMaxSize, flushIntervalMills, maxRetryTime);
		copy.keyFields = keyFields;
		return copy;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcUpsertTableSink) {
			JdbcUpsertTableSink sink = (JdbcUpsertTableSink) o;
			return Objects.equals(schema, sink.schema) &&
				Objects.equals(options, sink.options) &&
				Objects.equals(flushMaxSize, sink.flushMaxSize) &&
				Objects.equals(flushIntervalMills, sink.flushIntervalMills) &&
				Objects.equals(maxRetryTime, sink.maxRetryTime) &&
				Arrays.equals(keyFields, sink.keyFields) &&
				Objects.equals(isAppendOnly, sink.isAppendOnly);
		} else {
			return false;
		}
	}

	/**
	 * Builder for a {@link JdbcUpsertTableSink}.
	 */
	public static class Builder {
		protected TableSchema schema;
		private JdbcOptions options;
		protected int flushMaxSize = AbstractJdbcOutputFormat.DEFAULT_FLUSH_MAX_SIZE;
		protected long flushIntervalMills = AbstractJdbcOutputFormat.DEFAULT_FLUSH_INTERVAL_MILLS;
		protected int maxRetryTimes = JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES;

		/**
		 * required, table schema of this table source.
		 */
		public Builder setTableSchema(TableSchema schema) {
			this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
			return this;
		}

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JdbcOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setFlushMaxSize(int flushMaxSize) {
			this.flushMaxSize = flushMaxSize;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public JdbcUpsertTableSink build() {
			checkNotNull(schema, "No schema supplied.");
			checkNotNull(options, "No options supplied.");
			return new JdbcUpsertTableSink(schema, options, flushMaxSize, flushIntervalMills, maxRetryTimes);
		}
	}
}
