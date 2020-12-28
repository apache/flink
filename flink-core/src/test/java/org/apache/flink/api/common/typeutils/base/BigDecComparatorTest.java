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

import java.math.BigDecimal;

public class BigDecComparatorTest extends ComparatorTestBase<BigDecimal> {

    @Override
    protected TypeComparator<BigDecimal> createComparator(boolean ascending) {
        return new BigDecComparator(ascending);
    }

    @Override
    protected TypeSerializer<BigDecimal> createSerializer() {
        return new BigDecSerializer();
    }

    @Override
    protected BigDecimal[] getSortedTestData() {
        return new BigDecimal[] {
            new BigDecimal("-12.5E1000"),
            new BigDecimal("-12.5E100"),
            BigDecimal.valueOf(-12E100),
            BigDecimal.valueOf(-10000),
            BigDecimal.valueOf(-1.1),
            BigDecimal.valueOf(-1),
            BigDecimal.valueOf(-0.44),
            BigDecimal.ZERO,
            new BigDecimal("0.000000000000000000000000001"),
            new BigDecimal("0.0000001"),
            new BigDecimal("0.1234123413478523984729447"),
            BigDecimal.valueOf(1),
            BigDecimal.valueOf(1.1),
            BigDecimal.TEN,
            new BigDecimal("10000"),
            BigDecimal.valueOf(12E100),
            new BigDecimal("12.5E100"),
            new BigDecimal("10E100000"),
            new BigDecimal("10E1000000000")
        };
    }
}
