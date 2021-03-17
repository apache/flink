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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.IntValue;

/** Tests for {@link IntValueArrayComparator}. */
public class IntValueArrayComparatorTest extends ComparatorTestBase<IntValueArray> {

    @Override
    protected TypeComparator<IntValueArray> createComparator(boolean ascending) {
        return new IntValueArrayComparator(ascending);
    }

    @Override
    protected TypeSerializer<IntValueArray> createSerializer() {
        return new IntValueArraySerializer();
    }

    @Override
    protected IntValueArray[] getSortedTestData() {
        IntValueArray iva0 = new IntValueArray();

        IntValueArray iva1 = new IntValueArray();
        iva1.add(new IntValue(5));

        IntValueArray iva2 = new IntValueArray();
        iva2.add(new IntValue(5));
        iva2.add(new IntValue(10));

        return new IntValueArray[] {iva0, iva1, iva2};
    }
}
