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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utils for {@link TestValuesTableFactory}. */
public class TestValuesFactoryUtils {
    public static int validateAndExtractRowtimeIndex(
            CatalogTable sinkTable, boolean dropLateEvent, boolean isInsertOnly) {
        if (!dropLateEvent) {
            return -1;
        } else if (!isInsertOnly) {
            throw new ValidationException(
                    "Option 'sink.drop-late-event' only works for insert-only sink now.");
        }
        TableSchema schema = sinkTable.getSchema();
        List<WatermarkSpec> watermarkSpecs = schema.getWatermarkSpecs();
        if (watermarkSpecs.size() == 0) {
            throw new ValidationException(
                    "Please define the watermark in the schema that is used to indicate the rowtime column. "
                            + "The sink function will compare the rowtime and the current watermark to determine whether the event is late.");
        }
        String rowtimeName = watermarkSpecs.get(0).getRowtimeAttribute();
        return Arrays.asList(schema.getFieldNames()).indexOf(rowtimeName);
    }

    public static List<Map<String, String>> parsePartitionList(List<String> stringPartitions) {
        return stringPartitions.stream()
                .map(
                        partition -> {
                            Map<String, String> spec = new HashMap<>();
                            Arrays.stream(partition.split(","))
                                    .forEach(
                                            pair -> {
                                                String[] split = pair.split(":");
                                                spec.put(split[0].trim(), split[1].trim());
                                            });
                            return spec;
                        })
                .collect(Collectors.toList());
    }

    public static Map<Map<String, String>, Collection<Row>> mapPartitionToRow(
            DataType producedDataType, Collection<Row> rows, List<Map<String, String>> partitions) {
        Map<Map<String, String>, Collection<Row>> map = new HashMap<>();
        for (Map<String, String> partition : partitions) {
            map.put(partition, new ArrayList<>());
        }
        List<String> fieldNames = DataTypeUtils.flattenToNames(producedDataType);
        for (Row row : rows) {
            for (Map<String, String> partition : partitions) {
                boolean match = true;
                for (Map.Entry<String, String> entry : partition.entrySet()) {
                    int index = fieldNames.indexOf(entry.getKey());
                    if (index < 0) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Illegal partition list: partition key %s is not found in schema.",
                                        entry.getKey()));
                    }
                    if (entry.getValue() != null) {
                        if (row.getField(index) == null) {
                            match = false;
                        } else {
                            match =
                                    entry.getValue()
                                            .equals(
                                                    Objects.requireNonNull(row.getField(index))
                                                            .toString());
                        }
                    } else {
                        match = row.getField(index) == null;
                    }
                    if (!match) {
                        break;
                    }
                }
                if (match) {
                    map.get(partition).add(row);
                    break;
                }
            }
        }
        return map;
    }

    public static ChangelogMode parseChangelogMode(String string) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (String split : string.split(",")) {
            switch (split.trim()) {
                case "I":
                    builder.addContainedKind(RowKind.INSERT);
                    break;
                case "UB":
                    builder.addContainedKind(RowKind.UPDATE_BEFORE);
                    break;
                case "UA":
                    builder.addContainedKind(RowKind.UPDATE_AFTER);
                    break;
                case "D":
                    builder.addContainedKind(RowKind.DELETE);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid ChangelogMode string: " + string);
            }
        }
        return builder.build();
    }

    public static Map<String, DataType> convertToMetadataMap(
            Map<String, String> metadataOption, ClassLoader classLoader) {
        return metadataOption.keySet().stream()
                .sorted()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                key -> {
                                    final String typeString = metadataOption.get(key);
                                    final LogicalType type =
                                            LogicalTypeParser.parse(typeString, classLoader);
                                    return TypeConversions.fromLogicalToDataType(type);
                                },
                                (u, v) -> {
                                    throw new IllegalStateException();
                                },
                                LinkedHashMap::new));
    }
}
