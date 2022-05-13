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

import java.util.Random;

public class IntComparatorTest extends ComparatorTestBase<Integer> {

    @Override
    protected TypeComparator<Integer> createComparator(boolean ascending) {
        return new IntComparator(ascending);
    }

    @Override
    protected TypeSerializer<Integer> createSerializer() {
        return new IntSerializer();
    }

    @Override
    protected Integer[] getSortedTestData() {

        Random rnd = new Random(874597969123412338L);
        int rndInt = rnd.nextInt();
        if (rndInt < 0) {
            rndInt = -rndInt;
        }
        if (rndInt == Integer.MAX_VALUE) {
            rndInt -= 3;
        }
        if (rndInt <= 2) {
            rndInt += 3;
        }
        return new Integer[] {
            Integer.valueOf(Integer.MIN_VALUE),
            Integer.valueOf(-rndInt),
            Integer.valueOf(-1),
            Integer.valueOf(0),
            Integer.valueOf(1),
            Integer.valueOf(2),
            Integer.valueOf(rndInt),
            Integer.valueOf(Integer.MAX_VALUE)
        };
    }
}
