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

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/** A test for the {@link SetSerializer}. */
class SetSerializerTest extends SerializerTestBase<Set<Long>> {

    @Override
    protected TypeSerializer<Set<Long>> createSerializer() {
        return new SetSerializer<>(LongSerializer.INSTANCE);
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

        // empty sets
        final Set<Long> set1 = Collections.emptySet();
        final Set<Long> set2 = new HashSet<>();
        final Set<Long> set3 = new TreeSet<>();

        // single element sets
        final Set<Long> set4 = Collections.singleton(55L);
        final Set<Long> set5 = new HashSet<>();
        set5.add(12345L);
        final Set<Long> set6 = new TreeSet<>();
        set6.add(777888L);

        // longer sets
        final Set<Long> set7 = new HashSet<>();
        for (int i = 0; i < rnd.nextInt(200); i++) {
            set7.add(rnd.nextLong());
        }

        int set8Len = rnd.nextInt(200);
        final Set<Long> set8 = new HashSet<>(set8Len);
        for (int i = 0; i < set8Len; i++) {
            set8.add(rnd.nextLong());
        }

        // null-value sets
        final Set<Long> set9 = Collections.singleton(null);
        final Set<Long> set10 = new HashSet<>();
        set10.add(null);

        return (Set<Long>[])
                new Set[] {set1, set2, set3, set4, set5, set6, set7, set8, set9, set10};
    }
}
