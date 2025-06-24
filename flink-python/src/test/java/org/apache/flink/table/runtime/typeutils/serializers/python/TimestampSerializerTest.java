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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.sql.Timestamp;

/** Test for {@link TimestampSerializer}. */
abstract class TimestampSerializerTest extends SerializerTestBase<Timestamp> {

    @Override
    protected TypeSerializer<Timestamp> createSerializer() {
        return new TimestampSerializer(getPrecision());
    }

    @Override
    protected int getLength() {
        return (getPrecision() <= 3) ? 8 : 12;
    }

    @Override
    protected Class<Timestamp> getTypeClass() {
        return Timestamp.class;
    }

    abstract int getPrecision();

    @Override
    protected Timestamp[] getTestData() {
        return new Timestamp[] {Timestamp.valueOf("2018-03-11 03:00:00.123")};
    }

    static final class TimestampSerializerTest0 extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }
    }

    static final class TimestampSerializerTest3 extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }
    }

    static final class TimestampSerializerTest6 extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }
    }

    static final class TimestampSerializerTest8 extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }
    }
}
