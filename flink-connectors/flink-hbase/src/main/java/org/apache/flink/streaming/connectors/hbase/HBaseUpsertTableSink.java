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

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.Arrays;
import java.util.Map;

/**
 * Upsert table sink for HBase.
 */
public class HBaseUpsertTableSink implements UpsertStreamTableSink<Row> {

	/** Flag that indicates that only inserts are accepted. */
	private boolean isAppendOnly;

	/** Schema of the table. */
	private final TableSchema schema;

	private final HBaseTableBuilder tableBuilder;

	private final Map<String, String> userConfig;

	/** The rowkey field of each row. */
	private final String rowKeyField;

	private final String delimiter;

	/** Key field indices determined by the query. */
	private int[] keyFieldIndices = new int[0];

	public HBaseUpsertTableSink(
		TableSchema schema,
		HBaseTableBuilder tableBuilder,
		Map<String, String> userConfig,
		String rowKeyField,
		String delimiter) {
		this.schema = schema;
		this.tableBuilder = tableBuilder;
		this.userConfig = userConfig;
		this.rowKeyField = rowKeyField;
		this.delimiter = delimiter;
	}

	@Override
	public void setKeyFields(String[] keyNames) {
		// HBase update rely on rowkey
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		this.isAppendOnly = isAppendOnly;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		String[] fieldNames = getFieldNames();
		String[] columnFamilies = new String[fieldNames.length];
		String[] qualifiers = new String[fieldNames.length];
		int rowKeyIndex = -1;
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(rowKeyField)) {
				rowKeyIndex = i;
			} else {
				String[] split = fieldNames[i].split(delimiter);
				if (split.length != 2) {
					throw new RuntimeException("Column family and qualifer cannot be derived with field " + fieldNames[i]
						+ " and delimiter " + delimiter + ".");
				}
				columnFamilies[i] = split[0];
				qualifiers[i] = split[1];
			}
		}
		if (rowKeyIndex == -1) {
			throw new RuntimeException("Row key field " + rowKeyField + " cannot be found.");
		}

		final HBaseUpsertSinkFunction upsertSinkFunction =
			new HBaseUpsertSinkFunction(
				tableBuilder,
				userConfig,
				rowKeyIndex,
				getFieldNames(),
				columnFamilies,
				qualifiers,
				getFieldTypes(),
				(RowTypeInfo) getRecordType()
			);

		dataStream.addSink(upsertSinkFunction)
			.name(TableConnectorUtil.generateRuntimeName(this.getClass(), getFieldNames()));
	}

	@Override
	public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return Types.TUPLE(Types.BOOLEAN, getRecordType());
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
		return new HBaseUpsertTableSink(schema, tableBuilder, userConfig, rowKeyField, delimiter);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Sink to write row values into a HBase cluster in upsert mode.
	 */
	public static class HBaseUpsertSinkFunction extends HBaseSinkFunctionBase<Tuple2<Boolean, Row>> {

		public HBaseUpsertSinkFunction(
			HBaseTableBuilder tableBuilder,
			Map<String, String> userConfig,
			int rowKeyIndex,
			String[] fieldNames,
			String[] columnFamilies,
			String[] qualifiers,
			TypeInformation<?>[] fieldTypes,
			RowTypeInfo typeInfo) {
			super(tableBuilder, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
		}

		@Override
		protected Mutation extract(Tuple2<Boolean, Row> value) {
			if (value.f0) {
				return generatePutMutation(value);
			} else {
				return generateDeleteMutation(value);
			}
		}

		@Override
		protected Object produceElementWithIndex(Tuple2<Boolean, Row> value, int index) {
			return value.f1.getField(fieldElementIndexMapping[index]);
		}
	}

	/**
	 * Builder for {@link HBaseUpsertTableSink}.
	 */
	public static class Builder {
		private TableSchema schema;
		private HBaseTableBuilder tableBuilder;
		private Map<String, String> userConfig;
		private String rowKeyField;
		private String delimiter;

		public Builder setSchema(TableSchema schema) {
			this.schema = schema;
			return this;
		}

		public Builder setTableBuilder(HBaseTableBuilder tableBuilder) {
			this.tableBuilder = tableBuilder;
			return this;
		}

		public Builder setUserConfig(Map<String, String> userConfig) {
			this.userConfig = userConfig;
			return this;
		}

		public Builder setRowKeyField(String rowKeyField) {
			this.rowKeyField = rowKeyField;
			return this;
		}

		public Builder setDelimiter(String delimiter) {
			this.delimiter = delimiter;
			return this;
		}

		public HBaseUpsertTableSink build() {
			return new HBaseUpsertTableSink(schema, tableBuilder, userConfig, rowKeyField, delimiter);
		}
	}
}
