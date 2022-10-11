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

package org.apache.flink.connector.cassandra.source.split;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CassandraSplitSerializer}. */
public class CassandraSplitSerializerTest {

    @Test
    public void testSerdeRoundtrip() throws IOException {
        final CassandraSplit testData =
                new CassandraSplit(
                        ImmutableSet.of(
                                RingRange.of(BigInteger.ONE, BigInteger.TEN),
                                RingRange.of(BigInteger.ZERO, BigInteger.TEN)));
        final byte[] serialized = CassandraSplitSerializer.INSTANCE.serialize(testData);
        final CassandraSplit deserialized =
                CassandraSplitSerializer.INSTANCE.deserialize(
                        CassandraSplitSerializer.CURRENT_VERSION, serialized);
        assertThat(deserialized)
                .isEqualTo(testData)
                .withFailMessage(
                        "CassandraSplit is not the same as input object after serde roundtrip");
    }
}
