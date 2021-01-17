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

import java.sql.Timestamp;

public class SqlTimestampComparatorTest extends ComparatorTestBase<Timestamp> {

    @SuppressWarnings("unchecked")
    @Override
    protected TypeComparator<Timestamp> createComparator(boolean ascending) {
        return (TypeComparator) new SqlTimestampComparator(ascending);
    }

    @Override
    protected TypeSerializer<Timestamp> createSerializer() {
        return new SqlTimestampSerializer();
    }

    @Override
    protected Timestamp[] getSortedTestData() {
        return new Timestamp[] {
            Timestamp.valueOf("1970-01-01 00:00:00.000"),
            Timestamp.valueOf("1990-10-14 02:42:25.123"),
            Timestamp.valueOf("1990-10-14 02:42:25.123000001"),
            Timestamp.valueOf("1990-10-14 02:42:25.123000002"),
            Timestamp.valueOf("2013-08-12 14:15:59.478"),
            Timestamp.valueOf("2013-08-12 14:15:59.479"),
            Timestamp.valueOf("2040-05-12 18:00:45.999")
        };
    }
}
