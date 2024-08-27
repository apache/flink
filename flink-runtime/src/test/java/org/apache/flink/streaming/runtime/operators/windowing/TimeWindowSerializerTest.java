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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/** Tests for {@link TimeWindow.Serializer}. */
class TimeWindowSerializerTest extends SerializerTestBase<TimeWindow> {
    @Override
    protected TypeSerializer<TimeWindow> createSerializer() {
        return new TimeWindow.Serializer();
    }

    @Override
    protected int getLength() {
        return Long.BYTES * 2;
    }

    @Override
    protected Class<TimeWindow> getTypeClass() {
        return TimeWindow.class;
    }

    @Override
    protected TimeWindow[] getTestData() {
        return new TimeWindow[] {
            new TimeWindow(10, 20), new TimeWindow(100, 200), new TimeWindow(20, 21)
        };
    }
}
