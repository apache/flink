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

import java.util.Date;
import java.util.Random;

public class DateComparatorTest extends ComparatorTestBase<Date> {

    @Override
    protected TypeComparator<Date> createComparator(boolean ascending) {
        return new DateComparator(ascending);
    }

    @Override
    protected TypeSerializer<Date> createSerializer() {
        return new DateSerializer();
    }

    @Override
    protected Date[] getSortedTestData() {
        Random rnd = new Random(874597969123412338L);
        long rndLong = rnd.nextLong();
        if (rndLong < 0) {
            rndLong = -rndLong;
        }
        if (rndLong == Long.MAX_VALUE) {
            rndLong -= 3;
        }
        if (rndLong <= 2) {
            rndLong += 3;
        }
        return new Date[] {
            new Date(0L), new Date(1L), new Date(2L), new Date(rndLong), new Date(Long.MAX_VALUE)
        };
    }
}
