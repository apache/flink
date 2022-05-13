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

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.time.LocalTime;

public class LocalTimeComparatorTest extends ComparatorTestBase<LocalTime> {

    @Override
    protected TypeComparator<LocalTime> createComparator(boolean ascending) {
        return new LocalTimeComparator(ascending);
    }

    @Override
    protected TypeSerializer<LocalTime> createSerializer() {
        return new LocalTimeSerializer();
    }

    @Override
    protected LocalTime[] getSortedTestData() {
        return new LocalTime[] {
            LocalTime.of(0, 0, 0),
            LocalTime.of(2, 42, 25),
            LocalTime.of(14, 15, 59),
            LocalTime.of(18, 0, 45)
        };
    }
}
