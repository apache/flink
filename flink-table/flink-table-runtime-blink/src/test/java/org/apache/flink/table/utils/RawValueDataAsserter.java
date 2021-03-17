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

package org.apache.flink.table.utils;

import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;

/** A {@link org.hamcrest.Matcher} that allows equality check on {@link RawValueData}s. */
public class RawValueDataAsserter extends TypeSafeMatcher<RawValueData> {
    private final BinaryRawValueData expected;
    private final RawValueDataSerializer serializer;

    private RawValueDataAsserter(BinaryRawValueData expected, RawValueDataSerializer serializer) {
        this.expected = expected;
        this.serializer = serializer;
    }

    /**
     * Checks that the {@link RawValueData} is equivalent to the expected one. The serializer will
     * be used to ensure both objects are materialized into the binary form.
     *
     * @param expected the expected object
     * @param serializer serializer used to materialize the underlying java object
     * @return binary equality matcher
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static RawValueDataAsserter equivalent(
            RawValueData<?> expected, RawValueDataSerializer<?> serializer) {
        BinaryRawValueData binaryExpected = ((BinaryRawValueData) expected);
        binaryExpected.ensureMaterialized(serializer.getInnerSerializer());
        return new RawValueDataAsserter(binaryExpected, serializer);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected boolean matchesSafely(RawValueData item) {
        BinaryRawValueData binaryItem = (BinaryRawValueData) item;
        binaryItem.ensureMaterialized(serializer.getInnerSerializer());
        expected.ensureMaterialized(serializer.getInnerSerializer());

        return binaryItem.getSizeInBytes() == expected.getSizeInBytes()
                && BinarySegmentUtils.equals(
                        binaryItem.getSegments(),
                        binaryItem.getOffset(),
                        expected.getSegments(),
                        expected.getOffset(),
                        binaryItem.getSizeInBytes());
    }

    @Override
    public void describeTo(Description description) {
        byte[] bytes =
                BinarySegmentUtils.getBytes(
                        expected.getSegments(), expected.getOffset(), expected.getSizeInBytes());
        description.appendText(Arrays.toString(bytes));
    }
}
