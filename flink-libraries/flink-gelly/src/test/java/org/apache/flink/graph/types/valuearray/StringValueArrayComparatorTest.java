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
import org.apache.flink.types.StringValue;

/** Tests for {@link StringValueArrayComparator}. */
public class StringValueArrayComparatorTest extends ComparatorTestBase<StringValueArray> {

    @Override
    protected TypeComparator<StringValueArray> createComparator(boolean ascending) {
        return new StringValueArrayComparator(ascending);
    }

    @Override
    protected TypeSerializer<StringValueArray> createSerializer() {
        return new StringValueArraySerializer();
    }

    @Override
    protected StringValueArray[] getSortedTestData() {
        StringValueArray sva0 = new StringValueArray();

        StringValueArray sva1 = new StringValueArray();
        sva1.add(new StringValue("abc"));

        StringValueArray sva2 = new StringValueArray();
        sva2.add(new StringValue("qrs"));
        sva2.add(new StringValue("xyz"));

        return new StringValueArray[] {sva0, sva1, sva2};
    }
}
