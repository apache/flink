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

package org.apache.flink.table.module.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.hive.HiveAverageAggFunction;
import org.apache.flink.table.functions.hive.HiveCountAggFunction;
import org.apache.flink.table.functions.hive.HiveMaxAggFunction;
import org.apache.flink.table.functions.hive.HiveMinAggFunction;
import org.apache.flink.table.functions.hive.HiveSumAggFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.hive.udf.generic.GenericUDFLegacyGroupingID;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFArrayAccessStructField;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFInternalInterval;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFToDecimal;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Module to provide Hive built-in metadata. */
public class HiveModule implements Module {

    // a set of functions that shouldn't be overridden by HiveModule
    @VisibleForTesting
    static final Set<String> BUILT_IN_FUNC_BLACKLIST =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "cume_dist",
                                    "current_date",
                                    "current_timestamp",
                                    "current_database",
                                    "dense_rank",
                                    "first_value",
                                    "lag",
                                    "last_value",
                                    "lead",
                                    "ntile",
                                    "rank",
                                    "row_number",
                                    "hop",
                                    "hop_end",
                                    "hop_proctime",
                                    "hop_rowtime",
                                    "hop_start",
                                    "percent_rank",
                                    "session",
                                    "session_end",
                                    "session_proctime",
                                    "session_rowtime",
                                    "session_start",
                                    "tumble",
                                    "tumble_end",
                                    "tumble_proctime",
                                    "tumble_rowtime",
                                    "tumble_start")));

    static final Set<String> BUILTIN_NATIVE_AGG_FUNC =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList("sum", "count", "avg", "min", "max")));

    private final HiveFunctionDefinitionFactory factory;
    private final String hiveVersion;
    private final HiveShim hiveShim;
    private Set<String> functionNames;
    private final ReadableConfig config;
    private final ClassLoader classLoader;

    @VisibleForTesting
    public HiveModule() {
        this(
                HiveShimLoader.getHiveVersion(),
                new Configuration(),
                Thread.currentThread().getContextClassLoader());
    }

    @VisibleForTesting
    public HiveModule(String hiveVersion) {
        this(hiveVersion, Thread.currentThread().getContextClassLoader());
    }

    public HiveModule(String hiveVersion, ClassLoader classLoader) {
        this(hiveVersion, new Configuration(), classLoader);
    }

    public HiveModule(String hiveVersion, ReadableConfig config, ClassLoader classLoader) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(hiveVersion), "hiveVersion cannot be null");

        this.hiveVersion = hiveVersion;
        this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        this.factory = new HiveFunctionDefinitionFactory(hiveShim);
        this.functionNames = new HashSet<>();
        this.config = config;
        this.classLoader = classLoader;
    }

    @Override
    public Set<String> listFunctions() {
        // lazy initialize
        if (functionNames.isEmpty()) {
            functionNames = hiveShim.listBuiltInFunctions();
            functionNames.removeAll(BUILT_IN_FUNC_BLACKLIST);
            functionNames.add("grouping");
            functionNames.add(GenericUDFLegacyGroupingID.NAME);
            functionNames.add(HiveGenericUDFArrayAccessStructField.NAME);
            functionNames.add(HiveGenericUDFToDecimal.NAME);
        }
        return functionNames;
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        if (BUILT_IN_FUNC_BLACKLIST.contains(name)) {
            return Optional.empty();
        }
        FunctionDefinitionFactory.Context context = () -> classLoader;

        // We override some Hive's function by native implementation to supports hash-agg
        if (isNativeAggFunctionEnabled() && BUILTIN_NATIVE_AGG_FUNC.contains(name.toLowerCase())) {
            return getBuiltInNativeAggFunction(name.toLowerCase());
        }

        // We override Hive's grouping function. Refer to the implementation for more details.
        if (name.equalsIgnoreCase("grouping")) {
            return Optional.of(
                    factory.createFunctionDefinitionFromHiveFunction(
                            name, HiveGenericUDFGrouping.class.getName(), context));
        }

        // this function is used to generate legacy GROUPING__ID value for old hive versions
        if (name.equalsIgnoreCase(GenericUDFLegacyGroupingID.NAME)) {
            return Optional.of(
                    factory.createFunctionDefinitionFromHiveFunction(
                            name, GenericUDFLegacyGroupingID.class.getName(), context));
        }

        // We override Hive's internal_interval. Refer to the implementation for more details
        if (name.equalsIgnoreCase("internal_interval")) {
            return Optional.of(
                    factory.createFunctionDefinitionFromHiveFunction(
                            name, HiveGenericUDFInternalInterval.class.getName(), context));
        }

        // used to access the field of struct in array
        if (name.equalsIgnoreCase(HiveGenericUDFArrayAccessStructField.NAME)) {
            return Optional.of(
                    factory.createFunctionDefinitionFromHiveFunction(
                            name, HiveGenericUDFArrayAccessStructField.class.getName(), context));
        }

        // We add a custom to_decimal function. Refer to the implementation for more details.
        if (name.equalsIgnoreCase(HiveGenericUDFToDecimal.NAME)) {
            return Optional.of(
                    factory.createFunctionDefinitionFromHiveFunction(
                            name, HiveGenericUDFToDecimal.class.getName(), context));
        }

        Optional<FunctionInfo> info = hiveShim.getBuiltInFunctionInfo(name);

        return info.map(
                functionInfo ->
                        factory.createFunctionDefinitionFromHiveFunction(
                                name, functionInfo.getFunctionClass().getName(), context));
    }

    public String getHiveVersion() {
        return hiveVersion;
    }

    private boolean isNativeAggFunctionEnabled() {
        return config.get(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED);
    }

    private Optional<FunctionDefinition> getBuiltInNativeAggFunction(String name) {
        switch (name) {
            case "sum":
                // We override Hive's sum function by native implementation to support hash-agg
                return Optional.of(new HiveSumAggFunction());
            case "count":
                // We override Hive's sum function by native implementation to support hash-agg
                return Optional.of(new HiveCountAggFunction());
            case "avg":
                // We override Hive's avg function by native implementation to support hash-agg
                return Optional.of(new HiveAverageAggFunction());
            case "min":
                // We override Hive's min function by native implementation to support hash-agg
                return Optional.of(new HiveMinAggFunction());
            case "max":
                // We override Hive's max function by native implementation to support hash-agg
                return Optional.of(new HiveMaxAggFunction());
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Built-in hive aggregate function doesn't support %s yet!", name));
        }
    }
}
