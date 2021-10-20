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

import org.apache.flink.api.common.eventtime.Watermark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for {@link TestValuesTableFactory}. It can be used to collect global results from
 * sink and historical watermarks from the plan.
 */
public final class TestValuesRuntimeHelper {

    public static final Object LOCK = TestValuesTableFactory.class;

    // [table_name, [task_id, List[value]]]
    private static final Map<String, Map<Integer, List<String>>> globalRawResult = new HashMap<>();
    // [table_name, [task_id, Map[key, value]]]
    private static final Map<String, Map<Integer, Map<String, String>>> globalUpsertResult =
            new HashMap<>();
    // [table_name, [task_id, List[value]]]
    private static final Map<String, Map<Integer, List<String>>> globalRetractResult =
            new HashMap<>();
    // [table_name, [watermark]]
    private static final Map<String, List<Watermark>> watermarkHistory = new HashMap<>();

    public static Map<String, Map<Integer, List<String>>> getGlobalRawResult() {
        return globalRawResult;
    }

    public static Map<String, Map<Integer, Map<String, String>>> getGlobalUpsertResult() {
        return globalUpsertResult;
    }

    public static Map<String, Map<Integer, List<String>>> getGlobalRetractResult() {
        return globalRetractResult;
    }

    public static Map<String, List<Watermark>> getWatermarkHistory() {
        return watermarkHistory;
    }

    public static List<String> getRawResults(String tableName) {
        List<String> result = new ArrayList<>();
        synchronized (LOCK) {
            if (globalRawResult.containsKey(tableName)) {
                globalRawResult.get(tableName).values().forEach(result::addAll);
            }
        }
        return result;
    }

    /** Returns raw results if there was only one table with results, throws otherwise. */
    public static List<String> getOnlyRawResults() {
        List<String> result = new ArrayList<>();
        synchronized (LOCK) {
            if (globalRawResult.size() != 1) {
                throw new IllegalStateException(
                        "Expected results for only one table to be present, but found "
                                + globalRawResult.size());
            }

            globalRawResult.values().iterator().next().values().forEach(result::addAll);
        }
        return result;
    }

    public static List<Watermark> getWatermarks(String tableName) {
        synchronized (LOCK) {
            if (watermarkHistory.containsKey(tableName)) {
                return new ArrayList<>(watermarkHistory.get(tableName));
            } else {
                return Collections.emptyList();
            }
        }
    }

    public static List<String> getResults(String tableName) {
        List<String> result = new ArrayList<>();
        synchronized (LOCK) {
            if (globalUpsertResult.containsKey(tableName)) {
                globalUpsertResult
                        .get(tableName)
                        .values()
                        .forEach(map -> result.addAll(map.values()));
            } else if (globalRetractResult.containsKey(tableName)) {
                globalRetractResult.get(tableName).values().forEach(result::addAll);
            } else if (globalRawResult.containsKey(tableName)) {
                getRawResults(tableName).stream()
                        .map(s -> s.substring(3, s.length() - 1)) // removes the +I(...) wrapper
                        .forEach(result::add);
            }
        }
        return result;
    }

    public static void clearResults() {
        synchronized (LOCK) {
            globalRawResult.clear();
            globalUpsertResult.clear();
            globalRetractResult.clear();
            watermarkHistory.clear();
        }
    }
}
