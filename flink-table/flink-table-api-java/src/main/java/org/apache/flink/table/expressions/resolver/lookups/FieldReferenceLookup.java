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
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.operations.QueryOperation;

import java.util.Collections;
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

    private final List<Map<String, FieldReference>> fieldReferences;

    private static class FieldReference {
        final Column column;
        final int inputIdx;
        final int columnIdx;

        FieldReference(Column column, int inputIdx, int columnIdx) {
            this.column = column;
            this.inputIdx = inputIdx;
            this.columnIdx = columnIdx;
        }

        FieldReferenceExpression toExpr() {
            return new FieldReferenceExpression(
                    column.getName(), column.getDataType(), inputIdx, columnIdx);
        }
    }

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
        List<FieldReference> matchingFields =
                fieldReferences.stream()
                        .map(input -> input.get(name))
                        .filter(Objects::nonNull)
                        .collect(toList());

        if (matchingFields.size() == 1) {
            return Optional.of(matchingFields.get(0).toExpr());
        } else if (matchingFields.size() == 0) {
            return Optional.empty();
        } else {
            throw failAmbiguousColumn(name);
        }
    }

    /**
     * Gives all fields of underlying inputs in order of those inputs and order of fields within
     * input.
     *
     * @return concatenated list of fields of all inputs.
     */
    public List<FieldReferenceExpression> getAllInputFields() {
        return getInputFields(Collections.emptyList());
    }

    /**
     * Gives matching fields of underlying inputs in order of those inputs and order of fields
     * within input.
     *
     * @return concatenated list of matching fields of all inputs.
     */
    public List<FieldReferenceExpression> getInputFields(
            List<ColumnExpansionStrategy> expansionStrategies) {
        return fieldReferences.stream()
                .flatMap(input -> input.values().stream())
                .filter(fieldRef -> includeExpandedColumn(fieldRef.column, expansionStrategies))
                .map(FieldReference::toExpr)
                .collect(toList());
    }

    private static List<Map<String, FieldReference>> prepareFieldReferences(
            List<QueryOperation> queryOperations) {
        return IntStream.range(0, queryOperations.size())
                .mapToObj(idx -> prepareFieldsInInput(queryOperations.get(idx), idx))
                .collect(Collectors.toList());
    }

    private static Map<String, FieldReference> prepareFieldsInInput(
            QueryOperation input, int inputIdx) {
        ResolvedSchema resolvedSchema = input.getResolvedSchema();
        return IntStream.range(0, resolvedSchema.getColumnCount())
                .mapToObj(i -> new FieldReference(resolvedSchema.getColumns().get(i), inputIdx, i))
                .collect(
                        Collectors.toMap(
                                fieldRef -> fieldRef.column.getName(),
                                Function.identity(),
                                (fieldRef1, fieldRef2) -> {
                                    throw failAmbiguousColumn(fieldRef1.column.getName());
                                },
                                // we need to maintain order of fields within input for resolving
                                // e.g. '*' reference
                                LinkedHashMap::new));
    }

    private static ValidationException failAmbiguousColumn(String name) {
        return new ValidationException("Ambiguous column name: " + name);
    }

    // --------------------------------------------------------------------------------------------
    // Shared code with SQL validator
    // --------------------------------------------------------------------------------------------

    public static boolean includeExpandedColumn(
            Column column, List<ColumnExpansionStrategy> strategies) {
        for (ColumnExpansionStrategy strategy : strategies) {
            switch (strategy) {
                case EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS:
                    if (isAliasedVirtualMetadataColumn(column)) {
                        return false;
                    }
                    break;
                case EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS:
                    if (isDefaultVirtualMetadataColumn(column)) {
                        return false;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown column expansion strategy: " + strategy);
            }
        }
        return true;
    }

    private static boolean isAliasedVirtualMetadataColumn(Column column) {
        if (!(column instanceof Column.MetadataColumn)) {
            return false;
        }
        final Column.MetadataColumn metadataColumn = (Column.MetadataColumn) column;
        return metadataColumn.isVirtual() && metadataColumn.getMetadataKey().isPresent();
    }

    private static boolean isDefaultVirtualMetadataColumn(Column column) {
        if (!(column instanceof Column.MetadataColumn)) {
            return false;
        }
        final Column.MetadataColumn metadataColumn = (Column.MetadataColumn) column;
        return metadataColumn.isVirtual() && !metadataColumn.getMetadataKey().isPresent();
    }
}
