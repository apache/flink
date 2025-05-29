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
import org.apache.flink.api.java.typeutils.NullableSetTypeInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/** A test for the serializer of {@link NullableSetTypeInfo}. */
class NullableSetSerializerTest extends SerializerTestBase<Set<Long>> {

    @Override
    protected TypeSerializer<Set<Long>> createSerializer() {
        return new NullableSetTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO)
                .createSerializer(new SerializerConfigImpl());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<Set<Long>> getTypeClass() {
        return (Class<Set<Long>>) (Class<?>) Set.class;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected Set<Long>[] getTestData() {
        final Random rnd = new Random(123654789);

        // null set
        final Set<Long> set1 = null;

        // empty sets
        final Set<Long> set2 = Collections.emptySet();
        final Set<Long> set3 = new HashSet<>();

        // single element sets
        final Set<Long> set4 = Collections.singleton(55L);
        final Set<Long> set5 = new HashSet<>();
        set5.add(12345L);

        // longer sets with null value
        final Set<Long> set6 = new HashSet<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            set6.add(rnd.nextLong());
        }
        set6.add(null);

        return (Set<Long>[]) new Set[] {set1, set2, set3, set4, set5, set6};
    }
}
