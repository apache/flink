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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SplitsGenerator}. */
public final class SplitsGeneratorTest {

    @Test
    public void testGenerateSegments() {
        List<BigInteger> tokens =
                Stream.of(
                                "0",
                                "1",
                                "56713727820156410577229101238628035242",
                                "56713727820156410577229101238628035243",
                                "113427455640312821154458202477256070484",
                                "113427455640312821154458202477256070485")
                        .map(BigInteger::new)
                        .collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator("foo.bar.RandomPartitioner");
        List<CassandraSplit> segments = generator.generateSplits(10, tokens);

        assertThat(segments.size()).isEqualTo(12);
        assertThat(segments.get(0).getRingRanges().toString())
                .isEqualTo("[(0,1], (1,14178431955039102644307275309657008811]]");
        assertThat(segments.get(1).getRingRanges().toString())
                .isEqualTo(
                        "[(14178431955039102644307275309657008811,28356863910078205288614550619314017621]]");
        assertThat(segments.get(5).getRingRanges().toString())
                .isEqualTo(
                        "[(70892159775195513221536376548285044053,85070591730234615865843651857942052863]]");

        tokens =
                Stream.of(
                                "5",
                                "6",
                                "56713727820156410577229101238628035242",
                                "56713727820156410577229101238628035243",
                                "113427455640312821154458202477256070484",
                                "113427455640312821154458202477256070485")
                        .map(BigInteger::new)
                        .collect(Collectors.toList());

        segments = generator.generateSplits(10, tokens);

        assertThat(segments.size()).isEqualTo(12);
        assertThat(segments.get(0).getRingRanges().toString())
                .isEqualTo("[(5,6], (6,14178431955039102644307275309657008815]]");
        assertThat(segments.get(5).getRingRanges().toString())
                .isEqualTo(
                        "[(70892159775195513221536376548285044053,85070591730234615865843651857942052863]]");
        assertThat(segments.get(10).getRingRanges().toString())
                .isEqualTo(
                        "[(141784319550391026443072753096570088109,155962751505430129087380028406227096921]]");
    }

    @Test
    public void testZeroSizeRange() {
        List<String> tokenStrings =
                Arrays.asList(
                        "0",
                        "1",
                        "56713727820156410577229101238628035242",
                        "56713727820156410577229101238628035242",
                        "113427455640312821154458202477256070484",
                        "113427455640312821154458202477256070485");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator("foo.bar.RandomPartitioner");
        assertThatThrownBy(() -> generator.generateSplits(10, tokens))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testRotatedRing() {
        List<String> tokenStrings =
                Arrays.asList(
                        "56713727820156410577229101238628035243",
                        "113427455640312821154458202477256070484",
                        "113427455640312821154458202477256070485",
                        "5",
                        "6",
                        "56713727820156410577229101238628035242");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator("foo.bar.RandomPartitioner");
        List<CassandraSplit> splits = generator.generateSplits(5, tokens);
        assertThat(splits.size()).isEqualTo(6);
        assertThat(splits.get(1).getRingRanges())
                .containsExactlyInAnyOrder(
                        RingRange.of(
                                new BigInteger("85070591730234615865843651857942052863"),
                                new BigInteger("113427455640312821154458202477256070484")),
                        RingRange.of(
                                new BigInteger("113427455640312821154458202477256070484"),
                                new BigInteger("113427455640312821154458202477256070485")));

        assertThat(splits.get(2).getRingRanges())
                .containsExactlyInAnyOrder(
                        RingRange.of(
                                new BigInteger("113427455640312821154458202477256070485"),
                                new BigInteger("141784319550391026443072753096570088109")));

        assertThat(splits.get(3).getRingRanges())
                .containsExactlyInAnyOrder(
                        RingRange.of(
                                new BigInteger("141784319550391026443072753096570088109"),
                                new BigInteger("5")),
                        RingRange.of(new BigInteger("5"), new BigInteger("6")));
    }

    @Test
    public void testDisorderedRing() {
        List<String> tokenStrings =
                Arrays.asList(
                        "0",
                        "113427455640312821154458202477256070485",
                        "1",
                        "56713727820156410577229101238628035242",
                        "56713727820156410577229101238628035243",
                        "113427455640312821154458202477256070484");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator("foo.bar.RandomPartitioner");
        // Will throw an exception when concluding that the repair segments don't add up.
        // This is because the tokens were supplied out of order.
        assertThatThrownBy(() -> generator.generateSplits(10, tokens))
                .isInstanceOf(RuntimeException.class);
    }
}
