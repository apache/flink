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

public class DoubleComparatorTest extends ComparatorTestBase<Double> {

    @Override
    protected TypeComparator<Double> createComparator(boolean ascending) {
        return new DoubleComparator(ascending);
    }

    @Override
    protected TypeSerializer<Double> createSerializer() {
        return new DoubleSerializer();
    }

    @Override
    protected Double[] getSortedTestData() {
        Random rnd = new Random(874597969123412338L);
        double rndDouble = rnd.nextDouble();
        if (rndDouble < 0) {
            rndDouble = -rndDouble;
        }
        if (rndDouble == Double.MAX_VALUE) {
            rndDouble -= 3;
        }
        if (rndDouble <= 2) {
            rndDouble += 3;
        }
        return new Double[] {
            Double.valueOf(-rndDouble),
            Double.valueOf(-1.0D),
            Double.valueOf(0.0D),
            Double.valueOf(2.0D),
            Double.valueOf(rndDouble),
            Double.valueOf(Double.MAX_VALUE)
        };
    }
}
