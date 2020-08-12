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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;


/**
 * Use TestValuesCatalog to test partition push down.
 * */
public class TestValuesCatalog extends GenericInMemoryCatalog {
	private boolean supportListPartitionByFilter;
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
		TestValuesTableFactory.FilterUtil util = TestValuesTableFactory.FilterUtil.INSTANCE;
		List<CatalogPartitionSpec> remainingPartitions = new ArrayList<>();
		for (CatalogPartitionSpec partition : partitions) {
			boolean isRetained = true;
			Function<String, Comparable<?>> gettter = getGetter(partition.getPartitionSpec(), schema);
			for (Expression predicate : filters) {
				isRetained = util.isRetainedAfterApplyingFilterPredicates((ResolvedExpression) predicate, gettter);
				if (!isRetained) {
					break;
				}
			}
			if (isRetained) {
				remainingPartitions.add(partition);
			}
		}
		return remainingPartitions;
	}

	private Function<String, Comparable<?>> getGetter(Map<String, String> spec, TableSchema schema) {
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
			throw new UnsupportedOperationException(String.format("Unsupported data type: %s", type));
		}
	}
}
