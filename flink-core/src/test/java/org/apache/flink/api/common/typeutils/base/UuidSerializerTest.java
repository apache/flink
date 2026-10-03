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

import java.util.UUID;

/** A test for the {@link UuidSerializer}. */
class UuidSerializerTest extends SerializerTestBase<UUID> {

    @Override
    protected TypeSerializer<UUID> createSerializer() {
        return new UuidSerializer();
    }

    @Override
    protected int getLength() {
        return 16;
    }

    @Override
    protected Class<UUID> getTypeClass() {
        return UUID.class;
    }

    @Override
    protected UUID[] getTestData() {
        return new UUID[] {
            new UUID(0L, 0L),
            new UUID(0x8000000000000000L, 0x0000000000000000L),
            new UUID(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
            UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        };
    }
}
