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
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

class VariantSerializerTest extends SerializerTestBase<Variant> {

    @Override
    protected TypeSerializer<Variant> createSerializer() {
        return VariantSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Variant> getTypeClass() {
        return Variant.class;
    }

    @Override
    protected Variant[] getTestData() {
        VariantBuilder builder = Variant.newBuilder();
        return new Variant[] {
            builder.of(1),
            builder.object()
                    .add("k", builder.of(1))
                    .add("object", builder.object().add("k", builder.of("hello")).build())
                    .add(
                            "array",
                            builder.array()
                                    .add(builder.of(1))
                                    .add(builder.of(2))
                                    .add(builder.object().add("kk", builder.of(1.123f)).build())
                                    .build())
                    .build(),
            builder.array()
                    .add(builder.object().add("k", builder.of(1)).build())
                    .add(builder.of("hello"))
                    .add(builder.object().add("k", builder.of(2)).build())
                    .build()
        };
    }
}
