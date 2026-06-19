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
import org.apache.flink.api.java.typeutils.NullableListTypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** A test for the serializer of {@link NullableListTypeInfo}. */
class NullableListSerializerTest extends SerializerTestBase<List<Long>> {

    @Override
    protected TypeSerializer<List<Long>> createSerializer() {
        return new NullableListTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO)
                .createSerializer(new SerializerConfigImpl());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<List<Long>> getTypeClass() {
        return (Class<List<Long>>) (Class<?>) List.class;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected List<Long>[] getTestData() {
        final Random rnd = new Random(123654789);

        // null list
        final List<Long> list1 = null;

        // empty lists
        final List<Long> list2 = Collections.emptyList();
        final List<Long> list3 = new ArrayList<>();

        // single element lists
        final List<Long> list4 = Collections.singletonList(55L);
        final List<Long> list5 = new ArrayList<>();
        list5.add(777888L);

        // longer list with null value
        final List<Long> list6 = new ArrayList<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            list6.add(rnd.nextLong());
        }
        list6.add(null);

        return (List<Long>[]) new List[] {list1, list2, list3, list4, list5, list6};
    }
}
