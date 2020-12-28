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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/** Tests for {@link NullAwareMapSerializer}. */
public class NullAwareMapSerializerTest extends SerializerTestBase<Map<Long, String>> {
    @Override
    protected TypeSerializer<Map<Long, String>> createSerializer() {
        return new NullAwareMapSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Class<Map<Long, String>> getTypeClass() {
        return (Class<Map<Long, String>>) (Class) Map.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<Long, String>[] getTestData() {
        final Random rnd = new Random(123654789);

        // empty maps
        final Map<Long, String> map1 = Collections.emptyMap();
        final Map<Long, String> map2 = new HashMap<>();
        final Map<Long, String> map3 = new TreeMap<>();

        // single element maps
        final Map<Long, String> map4 = Collections.singletonMap(0L, "hello");
        final Map<Long, String> map5 = new HashMap<>();
        map5.put(12345L, "12345L");
        final Map<Long, String> map6 = new TreeMap<>();
        map6.put(777888L, "777888L");

        // longer maps
        final Map<Long, String> map7 = new HashMap<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            map7.put(rnd.nextLong(), Long.toString(rnd.nextLong()));
        }

        final Map<Long, String> map8 = new TreeMap<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            map8.put(rnd.nextLong(), Long.toString(rnd.nextLong()));
        }

        // null-value maps
        final Map<Long, String> map9 = Collections.singletonMap(0L, null);
        final Map<Long, String> map10 = new HashMap<>();
        map10.put(999L, null);
        final Map<Long, String> map11 = new TreeMap<>();
        map11.put(666L, null);

        // null-key maps
        final Map<Long, String> map12 = Collections.singletonMap(null, "");

        return (Map<Long, String>[])
                new Map[] {
                    map1, map2, map3, map4, map5, map6, map7, map8, map9, map10, map11, map12
                };
    }
}
