/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.stream.Stream;

/** Tests for the {@link UniformShardAssigner}. */
@ExtendWith(TestLoggerExtension.class)
public class UniformShardAssignerTest {

    static Stream<Arguments> testCaseProvider() {
        BigInteger two = BigInteger.valueOf(2);
        BigInteger three = BigInteger.valueOf(3);
        // split the hash key range into thirds
        BigInteger maxHashKey = two.pow(128).subtract(BigInteger.ONE);
        BigInteger[] rangeBoundaries = {
            BigInteger.ZERO,
            maxHashKey.divide(three),
            maxHashKey.divide(three).multiply(two),
            maxHashKey
        };
        return Stream.of(
                Arguments.of(BigInteger.ZERO, BigInteger.ZERO, 3, 0),
                Arguments.of(rangeBoundaries[0], rangeBoundaries[1], 3, 0),
                Arguments.of(rangeBoundaries[1], rangeBoundaries[2], 3, 1),
                Arguments.of(rangeBoundaries[2], rangeBoundaries[3], 3, 2),
                Arguments.of(maxHashKey, maxHashKey, 3, 2));
    }

    @ParameterizedTest
    @MethodSource("testCaseProvider")
    public void testAssignment(
            BigInteger rangeStart, BigInteger rangeEnd, int nSubtasks, int expectedSubtask) {
        Shard shard =
                new Shard()
                        .withShardId("shardId-000000003378")
                        .withHashKeyRange(
                                new HashKeyRange()
                                        .withStartingHashKey(rangeStart.toString())
                                        .withEndingHashKey(rangeEnd.toString()));
        StreamShardHandle handle = new StreamShardHandle("", shard);
        // streamName = "" hashes to zero

        Assertions.assertEquals(
                expectedSubtask,
                Math.abs(new UniformShardAssigner().assign(handle, nSubtasks)) % nSubtasks);
    }
}
