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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

import java.util.LinkedList;
import java.util.Random;

/** A test for the {@link LinkedListSerializer}. */
public class LinkedListSerializerTest extends SerializerTestBase<LinkedList<Long>> {

    @Override
    protected TypeSerializer<LinkedList<Long>> createSerializer() {
        return new LinkedListSerializer<>(LongSerializer.INSTANCE);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<LinkedList<Long>> getTypeClass() {
        return (Class<LinkedList<Long>>) (Class<?>) LinkedList.class;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected LinkedList<Long>[] getTestData() {
        final Random rnd = new Random(123654789);

        // empty lists
        final LinkedList<Long> list1 = new LinkedList<>();

        // single element lists
        final LinkedList<Long> list2 = new LinkedList<>();
        list2.add(12345L);

        // longer lists
        final LinkedList<Long> list3 = new LinkedList<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            list3.add(rnd.nextLong());
        }

        final LinkedList<Long> list4 = new LinkedList<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            list4.add(rnd.nextLong());
        }

        // list with null values
        final LinkedList<Long> list5 = new LinkedList<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            list5.add(rnd.nextBoolean() ? null : rnd.nextLong());
        }

        return (LinkedList<Long>[]) new LinkedList[] {list1, list2, list3, list4, list5};
    }
}
