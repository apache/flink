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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableSourceParserFactory;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.AbstractTableSource;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.IndexKey;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Factory of collection table which is only used for testing now.
 */
public class CollectionTableFactory<T1> implements StreamTableSourceFactory<T1>,
	StreamTableSinkFactory<Row>,
	TableSourceParserFactory,
	BatchTableSinkFactory<Row>,
	BatchTableSourceFactory<Row> {

	protected ClassLoader classLoader;

	public void setClassLoader(ClassLoader cl) {
		if (classLoader != null) {
			this.classLoader = cl;
		}
	}

	public static final List<Row> DATA = new LinkedList<>();

	public static final List<Row> RESULT = new LinkedList<>();

	public static TypeInformation rowType = null;

	public static TableSourceParser parser = null;

	public static RowTypeInfo sinkType = null;

	public static long emitIntervalMs = 1000;

	public static boolean checkParam = false;

	public static final String TABLE_TYPE_KEY = "tabletype";
	public static final int SOURCE = 1;
	public static final int DIM = 2;
	public static final int SINK = 3;

	public static void initData(RowTypeInfo rowTypeInfo, Collection<Row> data) {
		CollectionTableFactory.RESULT.clear();
		CollectionTableFactory.DATA.clear();
		CollectionTableFactory.parser = null;
		CollectionTableFactory.sinkType = rowTypeInfo;
		CollectionTableFactory.rowType = rowTypeInfo;
		CollectionTableFactory.DATA.addAll(data);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "COLLECTION"); // COLLECTION
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> ret = new ArrayList<>();
		ret.add(TableProperties.TABLE_NAME);
		ret.add(SchemaValidator.SCHEMA());
		ret.add("tabletype");
		return ret;
	}

	@Override
	public StreamTableSource createStreamTableSource(Map<String, String> props) {
		return getCollectionSource(props);
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> props) {
		return getCollectionSink(props);
	}

	@Override
	public TableSourceParser createParser(
		String tableName, RichTableSchema schema, TableProperties properties) {
		return parser;
	}

	@Override
	public BatchTableSink<Row> createBatchTableSink(Map<String, String> props) {
		return getCollectionSink(props);
	}

	@Override
	public BatchTableSource<Row> createBatchTableSource(Map<String, String> props) {
		return getCollectionSource(props);
	}

	private CollectionTableSource getCollectionSource(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		String tableName = properties.readTableNameFromProperties();
		RichTableSchema schema = properties.readSchemaFromProperties(classLoader);

		if (checkParam) {
			Preconditions.checkArgument(properties.getInteger(TABLE_TYPE_KEY, -1) == SOURCE);
		}
		return new CollectionTableSource(tableName, schema);
	}

	private CollectionTableSink getCollectionSink(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		String tableName = properties.readTableNameFromProperties();
		if (checkParam) {
			Preconditions.checkArgument(properties.getInteger(TABLE_TYPE_KEY, -1) == SINK);
		}
		return new CollectionTableSink(tableName);
	}

	/**
	 * Dimension table source fetcher.
	 */
	public static class TemporalTableFetcher extends TableFunction<Row> {

		private int[] keys;

		public TemporalTableFetcher(int[] keys) {
			this.keys = keys;
		}

		public void eval(Object... values) throws Exception {
			for (Row data : DATA) {
				boolean matched = true;
				for (int i = 0; i < keys.length; i++) {
					Object dataField = data.getField(keys[i]);
					Object inputField = null;
					if (dataField instanceof String) {
						inputField = values[i].toString();
					} else if (dataField instanceof Integer) {
						inputField = values[i];
					}
					if (!dataField.equals(inputField)) {
						matched = false;
						break;
					}
				}
				if (matched) {
					Row row = new Row(data.getArity());
					for (int i = 0; i < data.getArity(); i++) {
						Object dataField = data.getField(i);
						row.setField(i, dataField);
					}
					collect(row);
				}
			}
		}
	}

	/**
	 * Collection inputFormat for testing.
	 */
	public static class TestCollectionInputFormat<T> extends CollectionInputFormat<T> {

		public TestCollectionInputFormat(Collection<T> dataSet, TypeSerializer<T> serializer) {
			super(dataSet, serializer);
		}

		public boolean reachedEnd() throws IOException {
			try {
				Thread.currentThread().sleep(emitIntervalMs);
			} catch (InterruptedException e) {
			}
			return super.reachedEnd();
		}
	}

	/**
	 * Table source of collection.
	 */
	public static class CollectionTableSource
		extends AbstractTableSource
		implements BatchTableSource<Row>, StreamTableSource<Row>, LookupableTableSource<Row> {

		private String name;
		private RichTableSchema schema;

		public CollectionTableSource(String name, RichTableSchema schema) {
			this.name = name;
			this.schema = schema;
		}

		@Override
		public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
			return streamEnv.createInput(
				new TestCollectionInputFormat<>(DATA,
					rowType.createSerializer(new ExecutionConfig())),
				rowType, name);
		}

		@Override
		public DataType getReturnType() {
			return new TypeInfoWrappedDataType(rowType);
		}

		@Override
		public TableSchema getTableSchema() {
			List<IndexKey> indexKeys = schema.toIndexKeys();
			TableSchema.Builder builder = TableSchemaUtil.builderFromDataType(getReturnType());
			String[] fieldNames = schema.getColumnNames();
			for (IndexKey index : indexKeys) {
				int[] keys = index.toArray();
				String[] indexNames = new String[keys.length];
				for (int i = 0; i < keys.length; i++) {
					indexNames[i] = fieldNames[keys[i]];
				}
				builder.uniqueIndex(indexNames);
			}
			return builder.build();
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
			return new DataStream<>(execEnv, getBoundedStream(execEnv).getTransformation());
		}

		@Override
		public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
			return new TemporalTableFetcher(lookupKeys);
		}

		@Override
		public AsyncTableFunction<Row> getAsyncLookupFunction(int[] lookupKeys) {
			return null;
		}

		@Override
		public LookupConfig getLookupConfig() {
			return null;
		}
	}

	/**
	 * Sink function of unsafe memory.
	 */
	public static class UnsafeMemorySinkFunction extends RichSinkFunction<Row> {

		private TypeSerializer<Row> serializer;

		@Override
		public void open(Configuration param) {
			RESULT.clear();
			serializer = rowType.createSerializer(new ExecutionConfig());
		}

		@Override
		public void invoke(Row row) throws Exception {
			RESULT.add(serializer.copy(row));
		}
	}

	/**
	 * Table sink of collection.
	 */
	public static class CollectionTableSink
		implements BatchTableSink<Row>, AppendStreamTableSink<Row> {

		private String name;

		public CollectionTableSink(String name) {
			this.name = name;
		}

		@Override
		public DataStreamSink<Row> emitBoundedStream(
			DataStream<Row> boundedStream,
			TableConfig tableConfig, ExecutionConfig executionConfig) {
			DataStreamSink<Row> bounded = boundedStream.addSink(new UnsafeMemorySinkFunction())
				.name(name)
				.setParallelism(1);
			bounded.getTransformation().setMaxParallelism(1);
			return bounded;
		}

		@Override
		public DataType getOutputType() {
			return new TypeInfoWrappedDataType(sinkType);
		}

		@Override
		public String[] getFieldNames() {
			return sinkType.getFieldNames();
		}

		@Override
		public DataType[] getFieldTypes() {
			TypeInformation<?>[] typeInfos = sinkType.getFieldTypes();
			DataType[] types = new DataType[typeInfos.length];
			Arrays.setAll(types, i -> new TypeInfoWrappedDataType(typeInfos[i]));
			return types;
		}

		@Override
		public TableSink<Row> configure(String[] fieldNames, DataType[] fieldTypes) {
			return this;
		}

		@Override
		public DataStreamSink<Row> emitDataStream(DataStream<Row> dataStream) {
			return dataStream.addSink(new UnsafeMemorySinkFunction()).setParallelism(1);
		}
	}
}
