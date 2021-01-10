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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/** A test for the {@link LongSerializer}. */
public class ListSerializerTest extends SerializerTestBase<List<Long>> {

    @Override
    protected TypeSerializer<List<Long>> createSerializer() {
        return new ListSerializer<>(LongSerializer.INSTANCE);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected List<Long>[] getTestData() {
        final Random rnd = new Random(123654789);

        // empty lists
        final List<Long> list1 = Collections.emptyList();
        final List<Long> list2 = new LinkedList<>();
        final List<Long> list3 = new ArrayList<>();

        // single element lists
        final List<Long> list4 = Collections.singletonList(55L);
        final List<Long> list5 = new LinkedList<>();
        list5.add(12345L);
        final List<Long> list6 = new ArrayList<>();
        list6.add(777888L);

        // longer lists
        final List<Long> list7 = new LinkedList<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            list7.add(rnd.nextLong());
        }

        int list8Len = rnd.nextInt(200);
        final List<Long> list8 = new ArrayList<>(list8Len);
        for (int i = 0; i < list8Len; i++) {
            list8.add(rnd.nextLong());
        }

        return (List<Long>[]) new List[] {list1, list2, list3, list4, list5, list6, list7, list8};
    }
}
