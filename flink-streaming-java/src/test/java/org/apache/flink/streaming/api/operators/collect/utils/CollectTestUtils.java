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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Utilities for testing collecting mechanism. */
public class CollectTestUtils {

    public static <T> List<byte[]> toBytesList(List<T> values, TypeSerializer<T> serializer) {
        List<byte[]> ret = new ArrayList<>();
        for (T value : values) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            try {
                serializer.serialize(value, wrapper);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ret.add(baos.toByteArray());
        }
        return ret;
    }

    public static <T> void assertResponseEquals(
            CollectCoordinationResponse response,
            String version,
            long lastCheckpointedOffset,
            List<T> expected,
            TypeSerializer<T> serializer)
            throws IOException {
        Assert.assertEquals(version, response.getVersion());
        Assert.assertEquals(lastCheckpointedOffset, response.getLastCheckpointedOffset());
        List<T> results = response.getResults(serializer);
        assertResultsEqual(expected, results);
    }

    public static <T> void assertResultsEqual(List<T> expected, List<T> actual) {
        Assert.assertThat(actual, CoreMatchers.is(expected));
    }

    public static <T> void assertAccumulatorResult(
            Tuple2<Long, CollectCoordinationResponse> accResults,
            long expectedOffset,
            String expectedVersion,
            long expectedLastCheckpointedOffset,
            List<T> expectedResults,
            TypeSerializer<T> serializer)
            throws Exception {
        long offset = accResults.f0;
        CollectCoordinationResponse response = accResults.f1;
        List<T> actualResults = response.getResults(serializer);

        Assert.assertEquals(expectedOffset, offset);
        Assert.assertEquals(expectedVersion, response.getVersion());
        Assert.assertEquals(expectedLastCheckpointedOffset, response.getLastCheckpointedOffset());
        assertResultsEqual(expectedResults, actualResults);
    }
}
