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

package org.apache.flink.table.expressions.resolver.lookups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.operations.QueryOperation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/** Provides a way to look up field reference by the name of the field. */
@Internal
public class FieldReferenceLookup {

    private final List<Map<String, FieldReferenceExpression>> fieldReferences;

    public FieldReferenceLookup(List<QueryOperation> queryOperations) {
        fieldReferences = prepareFieldReferences(queryOperations);
    }

    /**
     * Tries to resolve {@link FieldReferenceExpression} using given name in underlying inputs.
     *
     * @param name name of field to look for
     * @return resolved field reference or empty if could not find field with given name.
     * @throws org.apache.flink.table.api.ValidationException if the name is ambiguous.
     */
    public Optional<FieldReferenceExpression> lookupField(String name) {
        List<FieldReferenceExpression> matchingFields =
                fieldReferences.stream()
                        .map(input -> input.get(name))
                        .filter(Objects::nonNull)
                        .collect(toList());

        if (matchingFields.size() == 1) {
            return Optional.of(matchingFields.get(0));
        } else if (matchingFields.size() == 0) {
            return Optional.empty();
        } else {
            throw failAmbiuguousColumn(name);
        }
    }

    /**
     * Gives all fields of underlying inputs in order of those inputs and order of fields within
     * input.
     *
     * @return concatenated list of fields of all inputs.
     */
    public List<FieldReferenceExpression> getAllInputFields() {
        return fieldReferences.stream().flatMap(input -> input.values().stream()).collect(toList());
    }

    private static List<Map<String, FieldReferenceExpression>> prepareFieldReferences(
            List<QueryOperation> queryOperations) {
        return IntStream.range(0, queryOperations.size())
                .mapToObj(idx -> prepareFieldsInInput(queryOperations.get(idx), idx))
                .collect(Collectors.toList());
    }

    private static Map<String, FieldReferenceExpression> prepareFieldsInInput(
            QueryOperation input, int inputIdx) {
        ResolvedSchema resolvedSchema = input.getResolvedSchema();
        return IntStream.range(0, resolvedSchema.getColumnCount())
                .mapToObj(
                        i ->
                                new FieldReferenceExpression(
                                        resolvedSchema.getColumnNames().get(i),
                                        resolvedSchema.getColumnDataTypes().get(i),
                                        inputIdx,
                                        i))
                .collect(
                        Collectors.toMap(
                                FieldReferenceExpression::getName,
                                Function.identity(),
                                (fieldRef1, fieldRef2) -> {
                                    throw failAmbiuguousColumn(fieldRef1.getName());
                                },
                                // we need to maintain order of fields within input for resolving
                                // e.g. '*' reference
                                LinkedHashMap::new));
    }

    private static ValidationException failAmbiuguousColumn(String name) {
        return new ValidationException("Ambiguous column name: " + name);
    }
}
