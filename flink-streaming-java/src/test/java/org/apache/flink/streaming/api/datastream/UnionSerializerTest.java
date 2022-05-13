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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.UnionSerializer;
import org.apache.flink.testutils.DeeplyEqualsChecker;

/** Serializer tests for {@link UnionSerializer}. */
public class UnionSerializerTest extends SerializerTestBase<TaggedUnion<Object, Object>> {

    public UnionSerializerTest() {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof TaggedUnion && o2 instanceof TaggedUnion,
                                (o1, o2, checker) -> {
                                    TaggedUnion union1 = (TaggedUnion) o1;
                                    TaggedUnion union2 = (TaggedUnion) o2;

                                    if (union1.isOne() && union2.isOne()) {
                                        return checker.deepEquals(union1.getOne(), union2.getOne());
                                    } else if (union1.isTwo() && union2.isTwo()) {
                                        return checker.deepEquals(union1.getTwo(), union2.getTwo());
                                    } else {
                                        return false;
                                    }
                                }));
    }

    @Override
    protected TypeSerializer<TaggedUnion<Object, Object>> createSerializer() {
        return new UnionSerializer<>(
                new KryoSerializer<>(Object.class, new ExecutionConfig()),
                new KryoSerializer<>(Object.class, new ExecutionConfig()));
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Class<TaggedUnion<Object, Object>> getTypeClass() {
        return (Class<TaggedUnion<Object, Object>>) (Class<?>) TaggedUnion.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TaggedUnion<Object, Object>[] getTestData() {
        return new TaggedUnion[] {TaggedUnion.one(1), TaggedUnion.two("A"), TaggedUnion.one("C")};
    }
}
