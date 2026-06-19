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
import org.apache.flink.table.data.TimestampData;

/** Test for {@link TimestampDataSerializer}. */
abstract class TimestampDataSerializerTest extends SerializerTestBase<TimestampData> {

    @Override
    protected TypeSerializer<TimestampData> createSerializer() {
        return new TimestampDataSerializer(getPrecision());
    }

    @Override
    protected int getLength() {
        return (getPrecision() <= 3) ? 8 : 12;
    }

    @Override
    protected Class<TimestampData> getTypeClass() {
        return TimestampData.class;
    }

    @Override
    protected TimestampData[] getTestData() {
        return new TimestampData[] {
            TimestampData.fromEpochMillis(1),
            TimestampData.fromEpochMillis(2),
            TimestampData.fromEpochMillis(3),
            TimestampData.fromEpochMillis(4)
        };
    }

    protected abstract int getPrecision();

    static final class TimestampSerializer0Test extends TimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }
    }

    static final class TimestampSerializer3Test extends TimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }
    }

    static final class TimestampSerializer6Test extends TimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }
    }

    static final class TimestampSerializer8Test extends TimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }
    }
}
