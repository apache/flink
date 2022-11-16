/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.util.SourceTestHarnessUtils;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link CollectionSource} test. */
public class CollectionSourceTest {

    @Test
    public void testCreateSourceWithNullElement() {
        List<String> collectionWithNull = Lists.newArrayList("1", null, "2");

        CommonTestUtils.assertThrows(
                "The collection contains a null element",
                RuntimeException.class,
                () -> {
                    new CollectionSource<>(collectionWithNull, StringSerializer.INSTANCE);
                    return null;
                });
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testSetOutputTypeWithIncompatibleType() {
        List<String> elements = Lists.newArrayList("1", "2");
        CollectionSource<String> source = new CollectionSource<>(elements);

        CommonTestUtils.assertThrows(
                "The elements in the collection are not all subclasses of",
                RuntimeException.class,
                () -> {
                    source.setOutputType((TypeInformation) Types.LONG, new ExecutionConfig());
                    return null;
                });
    }

    @Test
    public void testReadNonSerializableElements() throws Exception {
        List<NonSerializablePojo> elements =
                Lists.newArrayList(
                        new NonSerializablePojo(1, 2),
                        new NonSerializablePojo(3, 4),
                        new NonSerializablePojo(5, 6),
                        new NonSerializablePojo(7, 8));
        TypeSerializer<NonSerializablePojo> serializer =
                TypeExtractor.getForClass(NonSerializablePojo.class)
                        .createSerializer(new ExecutionConfig());

        CollectionSource<NonSerializablePojo> source = new CollectionSource<>(elements, serializer);
        List<NonSerializablePojo> result =
                SourceTestHarnessUtils.testBoundedSourceWithHarness(
                        source,
                        elements.size(),
                        Collections.singletonList(
                                new SerializedElementsSplit<>(
                                        source.getSerializedElements(),
                                        elements.size(),
                                        serializer)));
        assertEquals(elements, result);
    }

    @Test
    public void testReadSerializableElements() throws Exception {
        List<Integer> elements = IntStream.range(0, 10000).boxed().collect(Collectors.toList());
        CollectionSource<Integer> source = new CollectionSource<>(elements, IntSerializer.INSTANCE);

        List<Integer> result =
                SourceTestHarnessUtils.testBoundedSourceWithHarness(
                        source,
                        elements.size(),
                        Collections.singletonList(
                                new SerializedElementsSplit<>(
                                        source.getSerializedElements(),
                                        elements.size(),
                                        IntSerializer.INSTANCE)));
        assertEquals(elements, result);
    }

    private static class NonSerializablePojo {
        public long val1;
        public int val2;

        public NonSerializablePojo() {}

        public NonSerializablePojo(long val1, int val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NonSerializablePojo that = (NonSerializablePojo) o;
            return val1 == that.val1 && val2 == that.val2;
        }

        @Override
        public int hashCode() {
            return Objects.hash(val1, val2);
        }
    }
}
