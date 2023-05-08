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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Test suite for collection methods of {@link TtlMapState}. */
class TtlMapStateAllEntriesTestContext
        extends TtlMapStateTestContext<Map<Integer, String>, Set<Map.Entry<Integer, String>>> {

    @Override
    void initTestValues() {
        emptyValue = Collections.emptySet();

        updateEmpty =
                mapOf(
                        Tuple2.of(3, "3"),
                        Tuple2.of(5, "5"),
                        Tuple2.of(23, null),
                        Tuple2.of(10, "10"));
        updateUnexpired = mapOf(Tuple2.of(12, "12"), Tuple2.of(24, null), Tuple2.of(7, "7"));
        updateExpired = mapOf(Tuple2.of(15, "15"), Tuple2.of(25, null), Tuple2.of(4, "4"));

        getUpdateEmpty = updateEmpty.entrySet();
        getUnexpired = updateUnexpired.entrySet();
        getUpdateExpired = updateExpired.entrySet();
    }

    @SafeVarargs
    private static <UK, UV> Map<UK, UV> mapOf(Tuple2<UK, UV>... entries) {
        Map<UK, UV> map = new HashMap<>();
        Arrays.stream(entries).forEach(t -> map.put(t.f0, t.f1));
        return map;
    }

    @Override
    public void update(Map<Integer, String> map) throws Exception {
        ttlState.putAll(map);
    }

    @Override
    public Set<Map.Entry<Integer, String>> get() throws Exception {
        return StreamSupport.stream(ttlState.entries().spliterator(), false)
                .collect(Collectors.toSet());
    }

    @Override
    public Object getOriginal() throws Exception {
        return ttlState.original.entries() == null
                ? Collections.emptySet()
                : ttlState.original.entries();
    }

    @Override
    public boolean isOriginalEmptyValue() throws Exception {
        return Objects.equals(
                emptyValue, Sets.newHashSet(((Iterable<?>) getOriginal()).iterator()));
    }
}
