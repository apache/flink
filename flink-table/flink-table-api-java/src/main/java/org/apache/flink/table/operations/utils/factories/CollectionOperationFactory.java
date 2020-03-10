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

package org.apache.flink.table.operations.utils.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.CollectionQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeGeneralization;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.types.utils.ValueDataTypeConverter;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Utility class for creating valid {@link CollectionQueryOperation} operation.
 */
@Internal
public class CollectionOperationFactory {
	/**
	 * Create a valid {@link CollectionQueryOperation} operation.
	 */
	public QueryOperation create(List<?> elements) {
		List<List<?>> flattenElements = flattenRows(elements);
		DataType[] dataTypes = findRowTypes(flattenElements);
		String[] fieldNames = IntStream.range(0, dataTypes.length)
			.mapToObj(i -> "f" + i)
			.toArray(String[]::new);

		List<List<?>> castedElements = flattenElements.stream()
			.map(row -> convertToExpectType(row, dataTypes))
			.collect(Collectors.toList());

		TableSchema schema = TableSchema.builder()
			.fields(fieldNames, dataTypes)
			.build();
		return new CollectionQueryOperation(castedElements, schema);
	}

	public QueryOperation create(List<?> elements, DataType dataType) {
		TableSchema schema;
		if (dataType instanceof FieldsDataType) {
			FieldsDataType rowType = (FieldsDataType) dataType;
			RowType logicalRowType = (RowType) rowType.getLogicalType();
			schema = TableSchema.builder()
				.fields(
					logicalRowType.getFieldNames().toArray(new String[0]),
					logicalRowType.getFieldNames().stream()
						.map(name -> rowType.getFieldDataTypes().get(name))
						.toArray(DataType[]::new))
				.build();
		} else {
			schema = TableSchema.builder()
				.field("f0", dataType)
				.build();
		}

		List<List<?>> flattenElements =
			elements.isEmpty() ? Collections.emptyList() : flattenTopLevelRows(elements);

		return new CollectionQueryOperation(flattenElements, schema);
	}

	private List<List<?>> flattenRows(List<?> elements) {
		return elements.stream()
			.map(x -> flatten(x))
			.collect(Collectors.toList());
	}

	/**
	 * Flatten {@link Row}s and {@link Tuple}s.
	 */
	private <E> List<?> flatten(E element) {
		return Stream.of(element)
			.flatMap(e -> {
				if (e instanceof Row) {
					Row row = (Row) e;
					int arity = row.getArity();
					List<?> fields = IntStream.range(0, arity)
						.mapToObj(row::getField)
						.flatMap(x -> flatten(x).stream())
						.collect(Collectors.toList());
					return fields.stream();
				} else if (e instanceof Tuple) {
					Tuple tuple = (Tuple) e;
					int arity = tuple.getArity();
					List<?> fields = IntStream.range(0, arity)
						.mapToObj(tuple::getField)
						.flatMap(x -> flatten(x).stream())
						.collect(Collectors.toList());
					return fields.stream();
				} else {
					return Stream.of(e);
				}
			})
			.collect(Collectors.toList());
	}

	private List<List<?>> flattenTopLevelRows(List<?> elements) {
		return elements.stream()
			.map(x -> flattenTopLevel(x))
			.collect(Collectors.toList());
	}

	/**
	 * Flatten top level {@link Row}s and {@link Tuple}s.
	 */
	private <E> List<?> flattenTopLevel(E element) {
		return Stream.of(element)
			.flatMap(e -> {
				if (e instanceof Row) {
					Row row = (Row) e;
					int arity = row.getArity();
					List<?> fields = IntStream.range(0, arity)
						.mapToObj(row::getField)
						.collect(Collectors.toList());
					return fields.stream();
				} else if (e instanceof Tuple) {
					Tuple tuple = (Tuple) e;
					int arity = tuple.getArity();
					List<?> fields = IntStream.range(0, arity)
						.mapToObj(tuple::getField)
						.collect(Collectors.toList());
					return fields.stream();
				} else {
					return Stream.of(e);
				}
			})
			.collect(Collectors.toList());
	}

	private DataType[] findRowTypes(List<List<?>> rows) {
		int rowSize = rows.get(0).size();
		DataType[] dataTypes = new DataType[rowSize];
		IntStream.range(0, rowSize)
			.forEach(i -> {
				dataTypes[i] = findCommonTypeAtPosition(rows, i);
			});
		return dataTypes;
	}

	private DataType findCommonTypeAtPosition(List<List<?>> rows, int position) {
		List<LogicalType> typeAtPosition = extractLogicalTypesAtPosition(rows, position);
		LogicalType logicalType = LogicalTypeGeneralization.findCommonType(typeAtPosition)
			.orElseThrow(() -> new ValidationException(String.format(
				"Types in Collection must match. Could not find a common type at a %d-th position.",
				position)));
		return TypeConversions.fromLogicalToDataType(logicalType);
	}

	private List<LogicalType> extractLogicalTypesAtPosition(List<List<?>> rows, int position) {
		List<LogicalType> typesAtPosition = new ArrayList<>();
		rows.stream()
			.forEach(row -> {
				Optional<DataType> type = ValueDataTypeConverter.extractDataType(row.get(position));
				if (type.isPresent()) {
					typesAtPosition.add(type.get().getLogicalType());
				} else {
					typesAtPosition.add(extractNullTypeOrLegacyType(row, position).getLogicalType());
				}
			});
		return typesAtPosition;
	}

	private DataType extractNullTypeOrLegacyType(List<?> row, int position) {
		if (null == row.get(position)) {
			return DataTypes.NULL();
		}

		DataType type = null;
		try {
			type = TypeConversions.fromLegacyInfoToDataType(
				TypeExtractor.getForObject(row.get(position)));
		} catch (Exception e) {
			throw new ValidationException(String.format(
				"Types in Collection must be extractable. Could not extract type at $d-th position of %s.",
				position, Arrays.toString(row.toArray())), e);
		}
		return type;
	}

	private List<?> convertToExpectType(List<?> row, DataType[] dataTypes) {
		return IntStream.range(0, row.size())
			.mapToObj(i -> {
				Class<?> expectClazz = dataTypes[i].getConversionClass();
				boolean match = null == row.get(i) || row.get(i).getClass().equals(expectClazz);
				if (match) {
					return row.get(i);
				}

				// try to cast the object to expect type
				Object casted = valueLiteral(row.get(i)).getValueAs(expectClazz)
					.orElseThrow(() -> new ValidationException(String.format(
						"Could not cast %s to expected data type %s.",
						row.get(i), dataTypes[i])));
				return casted;
			})
			.collect(Collectors.toList());
	}
}
