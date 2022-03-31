/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.legacyutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A collector for storing results in memory. */
class RowCollector {
    private static final ArrayDeque<Tuple2<Boolean, Row>> sink = new ArrayDeque<>();

    public static void addValue(Tuple2<Boolean, Row> value) {
        synchronized (sink) {
            sink.add(value.copy());
        }
    }

    public static List<Tuple2<Boolean, Row>> getAndClearValues() {
        final ArrayList<Tuple2<Boolean, Row>> out = new ArrayList<>(sink);
        sink.clear();
        return out;
    }

    public static List<String> retractResults(List<Tuple2<Boolean, Row>> results) {
        final Map<String, Integer> retracted =
                results.stream()
                        .collect(
                                Collectors.groupingBy(
                                        r -> r.f1.toString(),
                                        Collectors.mapping(
                                                r -> r.f0 ? 1 : -1,
                                                Collectors.reducing(
                                                        0, (left, right) -> left + right))));

        if (retracted.values().stream().anyMatch(c -> c < 0)) {
            throw new AssertionError("Received retracted rows which have not been accumulated.");
        }

        return retracted.entrySet().stream()
                .flatMap(e -> IntStream.range(0, e.getValue()).mapToObj(i -> e.getKey()))
                .collect(Collectors.toList());
    }

    public static List<String> upsertResults(List<Tuple2<Boolean, Row>> results, int[] keys) {
        final HashMap<Row, String> upserted = new HashMap<>();
        for (Tuple2<Boolean, Row> r : results) {
            final Row key = Row.project(r.f1, keys);
            if (r.f0) {
                upserted.put(key, r.f1.toString());
            } else {
                upserted.remove(key);
            }
        }
        return new ArrayList<>(upserted.values());
    }
}
