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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * File system table source.
 */
public class FileSystemTableSource extends AbstractFileSystemTable implements
		ScanTableSource,
		SupportsProjectionPushDown,
		SupportsLimitPushDown,
		SupportsPartitionPushDown,
		SupportsFilterPushDown {

	private int[][] projectedFields;
	private List<Map<String, String>> remainingPartitions;
	private List<ResolvedExpression> filters;
	private Long limit;

	public FileSystemTableSource(DynamicTableFactory.Context context) {
		super(context);
	}

	private FileSystemTableSource(
			DynamicTableFactory.Context context,
			int[][] projectedFields,
			List<Map<String, String>> remainingPartitions,
			List<ResolvedExpression> filters,
			Long limit) {
		this(context);
		this.projectedFields = projectedFields;
		this.remainingPartitions = remainingPartitions;
		this.filters = filters;
		this.limit = limit;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return new DataStreamScanProvider() {
			@Override
			public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
				TypeInformation<RowData> typeInfo = InternalTypeInfo.of(
						getProducedDataType().getLogicalType());
				// Avoid using ContinuousFileMonitoringFunction
				return execEnv.addSource(
						new InputFormatSourceFunction<>(getInputFormat(), typeInfo),
						String.format("Filesystem(%s)", tableIdentifier),
						typeInfo);
			}

			@Override
			public boolean isBounded() {
				return true;
			}
		};
	}

	private InputFormat<RowData, ?> getInputFormat() {
		// When this table has no partition, just return a empty source.
		if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
			return new CollectionInputFormat<>(new ArrayList<>(), null);
		}

		FileSystemFormatFactory formatFactory = createFormatFactory(tableOptions);
		return formatFactory.createReader(new FileSystemFormatFactory.ReaderContext() {

			@Override
			public TableSchema getSchema() {
				return schema;
			}

			@Override
			public ReadableConfig getFormatOptions() {
				return new DelegatingConfiguration(tableOptions, formatFactory.factoryIdentifier() + ".");
			}

			@Override
			public List<String> getPartitionKeys() {
				return partitionKeys;
			}

			@Override
			public String getDefaultPartName() {
				return defaultPartName;
			}

			@Override
			public Path[] getPaths() {
				if (partitionKeys.isEmpty()) {
					return new Path[] {path};
				} else {
					return getOrFetchPartitions().stream()
							.map(FileSystemTableSource.this::toFullLinkedPartSpec)
							.map(PartitionPathUtils::generatePartitionPath)
							.map(n -> new Path(path, n))
							.toArray(Path[]::new);
				}
			}

			@Override
			public int[] getProjectFields() {
				return readFields();
			}

			@Override
			public long getPushedDownLimit() {
				return limit == null ? Long.MAX_VALUE : limit;
			}

			@Override
			public List<ResolvedExpression> getPushedDownFilters() {
				return filters == null ? Collections.emptyList() : filters;
			}
		});
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public Result applyFilters(List<ResolvedExpression> filters) {
		this.filters = filters;
		return Result.of(Collections.emptyList(), filters);
	}

	@Override
	public void applyLimit(long limit) {
		this.limit = limit;
	}

	@Override
	public Optional<List<Map<String, String>>> listPartitions() {
		try {
			return Optional.of(PartitionPathUtils
					.searchPartSpecAndPaths(path.getFileSystem(), path, partitionKeys.size())
					.stream()
					.map(tuple2 -> tuple2.f0)
					.map(spec -> {
						LinkedHashMap<String, String> ret = new LinkedHashMap<>();
						spec.forEach((k, v) -> ret.put(k, defaultPartName.equals(v) ? null : v));
						return ret;
					})
					.collect(Collectors.toList()));
		} catch (Exception e) {
			throw new TableException("Fetch partitions fail.", e);
		}
	}

	@Override
	public void applyPartitions(List<Map<String, String>> remainingPartitions) {
		this.remainingPartitions = remainingPartitions;
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.projectedFields = projectedFields;
	}

	@Override
	public FileSystemTableSource copy() {
		return new FileSystemTableSource(context, projectedFields, remainingPartitions, filters, limit);
	}

	@Override
	public String asSummaryString() {
		return "Filesystem";
	}

	private List<Map<String, String>> getOrFetchPartitions() {
		if (remainingPartitions == null) {
			remainingPartitions = listPartitions().get();
		}
		return remainingPartitions;
	}

	private LinkedHashMap<String, String> toFullLinkedPartSpec(Map<String, String> part) {
		LinkedHashMap<String, String> map = new LinkedHashMap<>();
		for (String k : partitionKeys) {
			if (!part.containsKey(k)) {
				throw new TableException("Partition keys are: " + partitionKeys +
						", incomplete partition spec: " + part);
			}
			map.put(k, part.get(k));
		}
		return map;
	}

	private int[] readFields() {
		return projectedFields == null ?
				IntStream.range(0, schema.getFieldCount()).toArray() :
				Arrays.stream(projectedFields).mapToInt(array -> array[0]).toArray();
	}

	private DataType getProducedDataType() {
		int[] fields = readFields();
		String[] schemaFieldNames = schema.getFieldNames();
		DataType[] schemaTypes = schema.getFieldDataTypes();

		return DataTypes.ROW(Arrays.stream(fields)
				.mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
				.toArray(DataTypes.Field[]::new))
				.bridgedTo(RowData.class);
	}
}
