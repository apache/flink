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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.tsextractors.TimestampExtractorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.table.utils.TypeMappingUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Logic to validate {@link TableSource} types. */
@Internal
public class TableSourceValidation {

    /**
     * Validates a TableSource.
     *
     * <ul>
     *   <li>checks that all fields of the schema can be resolved
     *   <li>checks that resolved fields have the correct type
     *   <li>checks that the time attributes are correctly configured.
     * </ul>
     *
     * @param tableSource The {@link TableSource} for which the time attributes are checked.
     */
    public static void validateTableSource(TableSource<?> tableSource, TableSchema schema) {
        List<RowtimeAttributeDescriptor> rowtimeAttributes = getRowtimeAttributes(tableSource);
        Optional<String> proctimeAttribute = getProctimeAttribute(tableSource);

        validateNoGeneratedColumns(schema);
        validateSingleRowtimeAttribute(rowtimeAttributes);
        validateRowtimeAttributesExistInSchema(rowtimeAttributes, schema);
        validateProctimeAttributesExistInSchema(proctimeAttribute, schema);
        validateLogicalToPhysicalMapping(tableSource, schema);
        validateTimestampExtractorArguments(rowtimeAttributes, tableSource);
        validateNotOverlapping(rowtimeAttributes, proctimeAttribute);
    }

    /**
     * Checks if the given {@link TableSource} defines a rowtime attribute.
     *
     * @param tableSource The table source to check.
     * @return true if the given table source defines rowtime attribute
     */
    public static boolean hasRowtimeAttribute(TableSource<?> tableSource) {
        return !getRowtimeAttributes(tableSource).isEmpty();
    }

    /**
     * Checks if the given {@link TableSource} defines a proctime attribute.
     *
     * @param tableSource The table source to check.
     * @return true if the given table source defines proctime attribute.
     */
    public static boolean hasProctimeAttribute(TableSource<?> tableSource) {
        return getProctimeAttribute(tableSource).isPresent();
    }

    private static void validateSingleRowtimeAttribute(
            List<RowtimeAttributeDescriptor> rowtimeAttributes) {
        if (rowtimeAttributes.size() > 1) {
            throw new ValidationException(
                    "Currently, only a single rowtime attribute is supported. "
                            + "Please remove all but one RowtimeAttributeDescriptor.");
        }
    }

    private static void validateRowtimeAttributesExistInSchema(
            List<RowtimeAttributeDescriptor> rowtimeAttributes, TableSchema tableSchema) {
        rowtimeAttributes.forEach(
                r -> {
                    if (!tableSchema.getFieldDataType(r.getAttributeName()).isPresent()) {
                        throw new ValidationException(
                                String.format(
                                        "Found a rowtime attribute for field '%s' but it does not exist in the Table. TableSchema: %s",
                                        r.getAttributeName(), tableSchema));
                    }
                });
    }

    private static void validateProctimeAttributesExistInSchema(
            Optional<String> proctimeAttribute, TableSchema tableSchema) {
        proctimeAttribute.ifPresent(
                r -> {
                    if (!tableSchema.getFieldDataType(r).isPresent()) {
                        throw new ValidationException(
                                String.format(
                                        "Found a proctime attribute for field '%s' but it does not exist in the Table. TableSchema: %s",
                                        r, tableSchema));
                    }
                });
    }

    private static void validateNotOverlapping(
            List<RowtimeAttributeDescriptor> rowtimeAttributes,
            Optional<String> proctimeAttribute) {
        proctimeAttribute.ifPresent(
                proctime -> {
                    if (rowtimeAttributes.stream()
                            .anyMatch(
                                    rowtimeAttribute ->
                                            rowtimeAttribute.getAttributeName().equals(proctime))) {
                        throw new ValidationException(
                                String.format(
                                        "Field '%s' must not be processing time and rowtime attribute at the same time.",
                                        proctime));
                    }
                });
    }

    private static void validateLogicalToPhysicalMapping(
            TableSource<?> tableSource, TableSchema schema) {
        final Function<String, String> fieldMapping = getNameMappingFunction(tableSource);

        // if we can
        TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
                tableSource,
                schema.getTableColumns(),
                true, // this makes no difference for validation, we don't care about the returned
                // indices
                fieldMapping);
    }

    private static Function<String, String> getNameMappingFunction(TableSource<?> tableSource) {
        final Function<String, String> fieldMapping;
        if (tableSource instanceof DefinedFieldMapping
                && ((DefinedFieldMapping) tableSource).getFieldMapping() != null) {
            Map<String, String> fieldsMap = ((DefinedFieldMapping) tableSource).getFieldMapping();
            if (fieldsMap != null) {
                fieldMapping = fieldsMap::get;
            } else {
                fieldMapping = Function.identity();
            }
        } else {
            fieldMapping = Function.identity();
        }
        return fieldMapping;
    }

    private static void validateTimestampExtractorArguments(
            List<RowtimeAttributeDescriptor> descriptors, TableSource<?> tableSource) {
        if (descriptors.size() == 1) {
            TimestampExtractor extractor = descriptors.get(0).getTimestampExtractor();
            TypeInformation<?>[] types =
                    Arrays.stream(
                                    TimestampExtractorUtils.getAccessedFields(
                                            extractor,
                                            tableSource.getProducedDataType(),
                                            getNameMappingFunction(tableSource)))
                            .map(ResolvedFieldReference::resultType)
                            .toArray(TypeInformation<?>[]::new);
            extractor.validateArgumentFields(types);
        }
    }

    private static void validateNoGeneratedColumns(TableSchema tableSchema) {
        if (!TableSchemaUtils.containsPhysicalColumnsOnly(tableSchema)) {
            throw new ValidationException(
                    "TableSource#getTableSchema should only contain physical columns, schema: \n"
                            + tableSchema);
        }
    }

    /** Returns a list with all rowtime attribute descriptors of the {@link TableSource}. */
    private static List<RowtimeAttributeDescriptor> getRowtimeAttributes(
            TableSource<?> tableSource) {
        if (tableSource instanceof DefinedRowtimeAttributes) {
            return ((DefinedRowtimeAttributes) tableSource).getRowtimeAttributeDescriptors();
        }

        return Collections.emptyList();
    }

    /** Returns the proctime attribute of the {@link TableSource} if it is defined. */
    private static Optional<String> getProctimeAttribute(TableSource<?> tableSource) {
        if (tableSource instanceof DefinedProctimeAttribute) {
            return Optional.ofNullable(
                    ((DefinedProctimeAttribute) tableSource).getProctimeAttribute());
        }

        return Optional.empty();
    }

    private TableSourceValidation() {}
}
