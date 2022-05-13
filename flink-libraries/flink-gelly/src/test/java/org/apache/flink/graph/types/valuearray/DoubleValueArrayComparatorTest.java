/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.DoubleValue;

/** Tests for {@link DoubleValueArrayComparator}. */
public class DoubleValueArrayComparatorTest extends ComparatorTestBase<DoubleValueArray> {

    @Override
    protected TypeComparator<DoubleValueArray> createComparator(boolean ascending) {
        return new DoubleValueArrayComparator(ascending);
    }

    @Override
    protected TypeSerializer<DoubleValueArray> createSerializer() {
        return new DoubleValueArraySerializer();
    }

    @Override
    protected DoubleValueArray[] getSortedTestData() {
        DoubleValueArray lva0 = new DoubleValueArray();

        DoubleValueArray lva1 = new DoubleValueArray();
        lva1.add(new DoubleValue(5));

        DoubleValueArray lva2 = new DoubleValueArray();
        lva2.add(new DoubleValue(5));
        lva2.add(new DoubleValue(10));

        return new DoubleValueArray[] {lva0, lva1};
    }
}
