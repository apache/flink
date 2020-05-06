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
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.filesystem.FileSystemTableFactory.createFormatFactory;

/**
 * File system table source.
 */
public class FileSystemTableSource extends InputFormatTableSource<RowData> implements
		PartitionableTableSource,
		ProjectableTableSource<RowData>,
		LimitableTableSource<RowData>,
		FilterableTableSource<RowData> {

	private final TableSchema schema;
	private final Path path;
	private final List<String> partitionKeys;
	private final String defaultPartName;
	private final Map<String, String> formatProperties;

	private final int[] selectFields;
	private final Long limit;
	private final List<Expression> filters;

	private List<Map<String, String>> readPartitions;

	/**
	 * Construct a file system table source.
	 *
	 * @param schema schema of the table.
	 * @param path directory path of the file system table.
	 * @param partitionKeys partition keys of the table.
	 * @param defaultPartName The default partition name in case the dynamic partition column value
	 *                        is null/empty string.
	 * @param formatProperties format properties.
	 */
	public FileSystemTableSource(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			String defaultPartName,
			Map<String, String> formatProperties) {
		this(schema, path, partitionKeys, defaultPartName, formatProperties, null, null, null, null);
	}

	private FileSystemTableSource(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			String defaultPartName,
			Map<String, String> formatProperties,
			List<Map<String, String>> readPartitions,
			int[] selectFields,
			Long limit,
			List<Expression> filters) {
		this.schema = schema;
		this.path = path;
		this.partitionKeys = partitionKeys;
		this.defaultPartName = defaultPartName;
		this.formatProperties = formatProperties;
		this.readPartitions = readPartitions;
		this.selectFields = selectFields;
		this.limit = limit;
		this.filters = filters;
	}

	@Override
	public InputFormat<RowData, ?> getInputFormat() {
		// When this table has no partition, just return a empty source.
		if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
			return new CollectionInputFormat<>(new ArrayList<>(), null);
		}

		return createFormatFactory(formatProperties).createReader(
				new FileSystemFormatFactory.ReaderContext() {

			@Override
			public TableSchema getSchema() {
				return schema;
			}

			@Override
			public Map<String, String> getFormatProperties() {
				return formatProperties;
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
			public List<Expression> getPushedDownFilters() {
				return filters == null ? Collections.emptyList() : filters;
			}
		});
	}

	private List<Map<String, String>> getOrFetchPartitions() {
		if (readPartitions == null) {
			readPartitions = getPartitions();
		}
		return readPartitions;
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

	@Override
	public List<Map<String, String>> getPartitions() {
		try {
			return PartitionPathUtils
					.searchPartSpecAndPaths(path.getFileSystem(), path, partitionKeys.size())
					.stream()
					.map(tuple2 -> tuple2.f0)
					.map(spec -> {
						LinkedHashMap<String, String> ret = new LinkedHashMap<>();
						spec.forEach((k, v) -> ret.put(k, defaultPartName.equals(v) ? null : v));
						return ret;
					})
					.collect(Collectors.toList());
		} catch (Exception e) {
			throw new TableException("Fetch partitions fail.", e);
		}
	}

	@Override
	public FileSystemTableSource applyPartitionPruning(
			List<Map<String, String>> remainingPartitions) {
		return new FileSystemTableSource(
				schema,
				path,
				partitionKeys,
				defaultPartName,
				formatProperties,
				remainingPartitions,
				selectFields,
				limit,
				filters);
	}

	@Override
	public FileSystemTableSource projectFields(int[] fields) {
		return new FileSystemTableSource(
				schema,
				path,
				partitionKeys,
				defaultPartName,
				formatProperties,
				readPartitions,
				fields,
				limit,
				filters);
	}

	@Override
	public FileSystemTableSource applyLimit(long limit) {
		return new FileSystemTableSource(
				schema,
				path,
				partitionKeys,
				defaultPartName,
				formatProperties,
				readPartitions,
				selectFields,
				limit,
				filters);
	}

	@Override
	public boolean isLimitPushedDown() {
		return limit != null;
	}

	@Override
	public FileSystemTableSource applyPredicate(List<Expression> predicates) {
		return new FileSystemTableSource(
				schema,
				path,
				partitionKeys,
				defaultPartName,
				formatProperties,
				readPartitions,
				selectFields,
				limit,
				new ArrayList<>(predicates));
	}

	@Override
	public boolean isFilterPushedDown() {
		return this.filters != null;
	}

	private int[] readFields() {
		return selectFields == null ?
				IntStream.range(0, schema.getFieldCount()).toArray() :
				selectFields;
	}

	@Override
	public DataType getProducedDataType() {
		int[] fields = readFields();
		String[] schemaFieldNames = schema.getFieldNames();
		DataType[] schemaTypes = schema.getFieldDataTypes();

		return DataTypes.ROW(Arrays.stream(fields)
				.mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
				.toArray(DataTypes.Field[]::new))
				.bridgedTo(RowData.class);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public String explainSource() {
		return super.explainSource() +
				(readPartitions == null ? "" : ", readPartitions=" + readPartitions) +
				(selectFields == null ? "" : ", selectFields=" + Arrays.toString(selectFields)) +
				(limit == null ? "" : ", limit=" + limit) +
				(filters == null ? "" : ", filters=" + filtersString());
	}

	private String filtersString() {
		return filters.stream().map(Expression::asSummaryString).collect(Collectors.joining(","));
	}
}
