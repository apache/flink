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

package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

/** Utility methods for dealing with {@link TimestampExtractor}. */
@Internal
public final class TimestampExtractorUtils {
    /**
     * Retrieves all field accesses needed for the given {@link TimestampExtractor}.
     *
     * @param timestampExtractor Extractor for which to construct array of field accesses.
     * @param physicalInputType Physical input type that the timestamp extractor accesses.
     * @param nameRemapping Additional remapping of a logical to a physical field name.
     *     TimestampExtractor works with logical names, but accesses physical fields
     * @return Array of physical field references.
     */
    public static ResolvedFieldReference[] getAccessedFields(
            TimestampExtractor timestampExtractor,
            DataType physicalInputType,
            Function<String, String> nameRemapping) {

        final Function<String, ResolvedFieldReference> fieldMapping;
        if (LogicalTypeChecks.isCompositeType(physicalInputType.getLogicalType())) {
            ResolvedSchema schema = DataTypeUtils.expandCompositeTypeToSchema(physicalInputType);
            fieldMapping = (arg) -> mapToResolvedField(nameRemapping, schema, arg);
        } else {
            fieldMapping =
                    (arg) ->
                            new ResolvedFieldReference(
                                    arg,
                                    TypeConversions.fromDataTypeToLegacyInfo(physicalInputType),
                                    0);
        }
        return getAccessedFields(timestampExtractor, fieldMapping);
    }

    private static ResolvedFieldReference[] getAccessedFields(
            TimestampExtractor timestampExtractor,
            Function<String, ResolvedFieldReference> fieldMapping) {
        return Arrays.stream(timestampExtractor.getArgumentFields())
                .map(fieldMapping)
                .toArray(ResolvedFieldReference[]::new);
    }

    private static ResolvedFieldReference mapToResolvedField(
            Function<String, String> nameRemapping, ResolvedSchema schema, String arg) {
        String remappedName = nameRemapping.apply(arg);

        int idx =
                IntStream.range(0, schema.getColumnCount())
                        .filter(i -> schema.getColumnNames().get(i).equals(remappedName))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Field %s does not exist", remappedName)));

        TypeInformation<?> dataType =
                TypeConversions.fromDataTypeToLegacyInfo(schema.getColumnDataTypes().get(idx));
        return new ResolvedFieldReference(remappedName, dataType, idx);
    }

    private TimestampExtractorUtils() {}
}
