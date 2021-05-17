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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.StringValue;

public class CopyableValueComparatorTest extends ComparatorTestBase<StringValue> {

    StringValue[] data =
            new StringValue[] {
                new StringValue(""),
                new StringValue("Lorem Ipsum Dolor Omit Longer"),
                new StringValue("aaaa"),
                new StringValue("abcd"),
                new StringValue("abce"),
                new StringValue("abdd"),
                new StringValue("accd"),
                new StringValue("bbcd")
            };

    @Override
    protected TypeComparator<StringValue> createComparator(boolean ascending) {
        return new CopyableValueComparator<StringValue>(ascending, StringValue.class);
    }

    @Override
    protected TypeSerializer<StringValue> createSerializer() {
        return new CopyableValueSerializer<StringValue>(StringValue.class);
    }

    @Override
    protected StringValue[] getSortedTestData() {
        return data;
    }
}
