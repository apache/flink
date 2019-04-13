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

package org.apache.flink.table.expressions.lookups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.operations.TableOperation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Provides a way to look up field reference by the name of the field.
 */
@Internal
public class FieldReferenceLookup {

	private final List<Map<String, FieldReferenceExpression>> fieldReferences;

	public FieldReferenceLookup(List<TableOperation> tableOperations) {
		fieldReferences = prepareFieldReferences(tableOperations);
	}

	/**
	 * Tries to resolve {@link FieldReferenceExpression} using given name in underlying inputs.
	 *
	 * @param name name of field to look for
	 * @return resolved field reference or empty if could not find field with given name.
	 * @throws org.apache.flink.table.api.ValidationException if the name is ambiguous.
	 */
	public Optional<FieldReferenceExpression> lookupField(String name) {
		List<FieldReferenceExpression> matchingFields = fieldReferences.stream()
			.map(input -> input.get(name))
			.filter(Objects::nonNull)
			.collect(toList());

		if (matchingFields.size() == 1) {
			return Optional.of(matchingFields.get(0));
		} else if (matchingFields.size() == 0) {
			return Optional.empty();
		} else {
			throw failAmbigousColumn(name);
		}
	}

	/**
	 * Gives all fields of underlying inputs in order of those inputs and order of fields within input.
	 *
	 * @return concatenated list of fields of all inputs.
	 */
	public List<FieldReferenceExpression> getAllInputFields() {
		return fieldReferences.stream().flatMap(input -> input.values().stream()).collect(toList());
	}

	private static List<Map<String, FieldReferenceExpression>> prepareFieldReferences(
			List<TableOperation> tableOperations) {
		return IntStream.range(0, tableOperations.size())
			.mapToObj(idx -> prepareFieldsInInput(tableOperations.get(idx), idx))
			.collect(Collectors.toList());
	}

	private static Map<String, FieldReferenceExpression> prepareFieldsInInput(TableOperation input, int inputIdx) {
		TableSchema tableSchema = input.getTableSchema();
		return IntStream.range(0, tableSchema.getFieldCount())
			.mapToObj(i -> new FieldReferenceExpression(
				tableSchema.getFieldName(i).get(),
				tableSchema.getFieldType(i).get(),
				inputIdx,
				i))
			.collect(Collectors.toMap(
				FieldReferenceExpression::getName,
				Function.identity(),
				(fieldRef1, fieldRef2) -> {
					throw failAmbigousColumn(fieldRef1.getName());
				},
				// we need to maintain order of fields within input for resolving e.g. '*' reference
				LinkedHashMap::new
			));
	}

	private static ValidationException failAmbigousColumn(String name) {
		return new ValidationException("Ambigous column name: " + name);
	}
}
