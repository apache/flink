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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.NullableMapTypeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/** A test for the serializer of {@link NullableMapTypeInfo}. */
class NullableMapSerializerTest extends SerializerTestBase<Map<Long, String>> {

    @Override
    protected TypeSerializer<Map<Long, String>> createSerializer() {
        return new NullableMapTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                .createSerializer(new SerializerConfigImpl());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<Map<Long, String>> getTypeClass() {
        return (Class<Map<Long, String>>) (Class<?>) Map.class;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected Map<Long, String>[] getTestData() {
        final Random rnd = new Random(123654789);

        // null map
        final Map<Long, String> map1 = null;

        // empty maps
        final Map<Long, String> map2 = Collections.emptyMap();
        final Map<Long, String> map3 = new HashMap<>();

        // single element maps
        final Map<Long, String> map4 = Collections.singletonMap(0L, "hello");
        final Map<Long, String> map5 = new HashMap<>();
        map5.put(12345L, "12345L");

        // longer maps with null key and null value
        final Map<Long, String> map6 = new HashMap<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            map6.put(rnd.nextLong(), Long.toString(rnd.nextLong()));
        }
        map6.put(rnd.nextLong(), null);
        map6.put(null, Long.toString(rnd.nextLong()));

        return (Map<Long, String>[]) new Map[] {map1, map2, map3, map4, map5, map6};
    }
}
