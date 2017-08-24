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

import org.apache.commons.collections4.multiset.AbstractMultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Random;

/**
 * Test for {@link MultisetSerializer}.
 */
public class MultisetSerializerTest extends SerializerTestBase<AbstractMultiSet<Long>> {

  @Override
  protected TypeSerializer<AbstractMultiSet<Long>> createSerializer() {
    return new MultisetSerializer(LongSerializer.INSTANCE);
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Class<AbstractMultiSet<Long>> getTypeClass() {
    return (Class<AbstractMultiSet<Long>>) (Class<?>) AbstractMultiSet.class;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected AbstractMultiSet<Long>[] getTestData() {
    final Random rnd = new Random(123654789);

    // empty Multisets
    final AbstractMultiSet<Long> set1 = new HashMultiSet<>();

    // single element Multisets
    final AbstractMultiSet<Long> set2 = new HashMultiSet<>();
    set2.add(12345L);

    // larger Multisets
    final AbstractMultiSet<Long> set3 = new HashMultiSet<>();
    for (int i = 0; i < rnd.nextInt(200); i++) {
      set3.add(rnd.nextLong());
    }

    return (AbstractMultiSet<Long>[]) new AbstractMultiSet[]{
        set1, set2, set3
    };
  }
}
