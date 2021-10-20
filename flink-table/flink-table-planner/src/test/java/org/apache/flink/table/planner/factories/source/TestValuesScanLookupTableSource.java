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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Values {@link LookupTableSource} and {@link ScanTableSource} for testing.
 *
 * <p>Note: we separate the implementations for scan and lookup to make it possible to support a
 * scan source without lookup ability, e.g. testing temporal join changelog source.
 */
public class TestValuesScanLookupTableSource extends TestValuesScanTableSource
        implements LookupTableSource {

    private final @Nullable String lookupFunctionClass;
    private final boolean isAsync;

    public TestValuesScanLookupTableSource(
            DataType producedDataType,
            ChangelogMode changelogMode,
            boolean bounded,
            String runtimeSource,
            boolean failingSource,
            Map<Map<String, String>, Collection<Row>> data,
            boolean isAsync,
            @Nullable String lookupFunctionClass,
            boolean nestedProjectionSupported,
            int[][] projectedFields,
            List<ResolvedExpression> filterPredicates,
            Set<String> filterableFields,
            int numElementToSkip,
            long limit,
            List<Map<String, String>> allPartitions,
            Map<String, DataType> readableMetadata,
            @Nullable int[] projectedMetadataFields) {
        super(
                producedDataType,
                changelogMode,
                bounded,
                runtimeSource,
                failingSource,
                data,
                nestedProjectionSupported,
                projectedFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
        this.lookupFunctionClass = lookupFunctionClass;
        this.isAsync = isAsync;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (lookupFunctionClass != null) {
            // use the specified lookup function
            try {
                Class<?> clazz = Class.forName(lookupFunctionClass);
                Object udtf = InstantiationUtil.instantiate(clazz);
                if (udtf instanceof TableFunction) {
                    return TableFunctionProvider.of((TableFunction) udtf);
                } else {
                    return AsyncTableFunctionProvider.of((AsyncTableFunction) udtf);
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                        "Could not instantiate class: " + lookupFunctionClass);
            }
        }

        int[] lookupIndices = Arrays.stream(context.getKeys()).mapToInt(k -> k[0]).toArray();
        Map<Row, List<Row>> mapping = new HashMap<>();
        Collection<Row> rows;
        if (allPartitions.equals(Collections.EMPTY_LIST)) {
            rows = data.getOrDefault(Collections.EMPTY_MAP, Collections.EMPTY_LIST);
        } else {
            rows = new ArrayList<>();
            allPartitions.forEach(key -> rows.addAll(data.getOrDefault(key, new ArrayList<>())));
        }

        List<Row> data = new ArrayList<>(rows);
        if (numElementToSkip > 0) {
            if (numElementToSkip >= data.size()) {
                data = Collections.EMPTY_LIST;
            } else {
                data = data.subList(numElementToSkip, data.size());
            }
        }

        data.forEach(
                record -> {
                    Row key =
                            Row.of(
                                    Arrays.stream(lookupIndices)
                                            .mapToObj(record::getField)
                                            .toArray());
                    List<Row> list = mapping.get(key);
                    if (list != null) {
                        list.add(record);
                    } else {
                        list = new ArrayList<>();
                        list.add(record);
                        mapping.put(key, list);
                    }
                });
        if (isAsync) {
            return AsyncTableFunctionProvider.of(new AsyncTestValueLookupFunction(mapping));
        } else {
            return TableFunctionProvider.of(new TestValuesLookupFunction(mapping));
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new TestValuesScanLookupTableSource(
                producedDataType,
                changelogMode,
                bounded,
                runtimeSource,
                failingSource,
                data,
                isAsync,
                lookupFunctionClass,
                nestedProjectionSupported,
                projectedPhysicalFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
    }
}
