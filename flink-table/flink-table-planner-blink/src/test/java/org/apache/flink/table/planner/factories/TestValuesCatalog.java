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

package org.apache.flink.table.planner.factories;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.utils.FilterUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Use TestValuesCatalog to test partition push down.
 * */
public class TestValuesCatalog extends GenericInMemoryCatalog {
	private final boolean supportListPartitionByFilter;
	public TestValuesCatalog(String name, String defaultDatabase, boolean supportListPartitionByFilter) {
		super(name, defaultDatabase);
		this.supportListPartitionByFilter = supportListPartitionByFilter;
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		if (!supportListPartitionByFilter) {
			throw new UnsupportedOperationException("TestValuesCatalog doesn't support list partition by filters");
		}

		List<CatalogPartitionSpec> partitions = listPartitions(tablePath);
		if (partitions.isEmpty()) {
			return partitions;
		}

		CatalogBaseTable table = this.getTable(tablePath);
		TableSchema schema = table.getSchema();
		List<ResolvedExpression> resolvedExpressions = filters.stream()
			.map(filter -> {
				if (filter instanceof ResolvedExpression) {
					return (ResolvedExpression) filter;
				}
				throw new UnsupportedOperationException(
					String.format("TestValuesCatalog only works with resolved expressions. Get unresolved expression: %s",
						filter));
			})
			.collect(Collectors.toList());

		return partitions.stream()
			.filter(
				partition -> {
					Function<String, Comparable<?>> getter = getValueGetter(partition.getPartitionSpec(), schema);
					return FilterUtils.isRetainedAfterApplyingFilterPredicates(resolvedExpressions, getter); })
			.collect(Collectors.toList());
	}

	private Function<String, Comparable<?>> getValueGetter(Map<String, String> spec, TableSchema schema) {
		return field -> {
			Optional<DataType> optionalDataType = schema.getFieldDataType(field);
			if (!optionalDataType.isPresent()) {
				throw new TableException(String.format("Field %s is not found in table schema.", field));
			}
			return convertValue(optionalDataType.get().getLogicalType(), spec.getOrDefault(field, null));
		};
	}

	private Comparable<?> convertValue(LogicalType type, String value) {
		if (type instanceof BooleanType) {
			return Boolean.valueOf(value);
		} else if (type instanceof CharType) {
			return value.charAt(0);
		} else if (type instanceof DoubleType) {
			return Double.valueOf(value);
		} else if (type instanceof IntType) {
			return Integer.valueOf(value);
		} else if (type instanceof VarCharType) {
			return value;
		} else {
			throw new UnsupportedOperationException(String.format("Unsupported data type: %s.", type));
		}
	}
}
