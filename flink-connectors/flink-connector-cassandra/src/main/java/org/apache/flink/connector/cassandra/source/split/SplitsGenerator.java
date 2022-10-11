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

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * This class generates {@link CassandraSplit}s by generating {@link RingRange}s based on Cassandra
 * cluster partitioner and Flink source parallelism.
 */
public final class SplitsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SplitsGenerator.class);

    private final String partitioner;
    private final BigInteger rangeMin;
    private final BigInteger rangeMax;
    private final BigInteger rangeSize;

    public SplitsGenerator(String partitioner) {
        this.partitioner = partitioner;
        rangeMin = getRangeMin();
        rangeMax = getRangeMax();
        rangeSize = getRangeSize();
    }

    private BigInteger getRangeMin() {
        if (partitioner.endsWith("RandomPartitioner")) {
            return BigInteger.ZERO;
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            return BigInteger.valueOf(2).pow(63).negate();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
        }
    }

    private BigInteger getRangeMax() {
        if (partitioner.endsWith("RandomPartitioner")) {
            return BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE);
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            return BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
        }
    }

    private BigInteger getRangeSize() {
        return rangeMax.subtract(rangeMin).add(BigInteger.ONE);
    }

    /**
     * Given properly ordered list of Cassandra tokens, compute at least {@code totalSplitCount}
     * splits. Each split can contain several token ranges in order to reduce the overhead of
     * Cassandra vnodes. Currently, token range grouping is not smart and doesn't check if they
     * share the same replicas.
     *
     * @param totalSplitCount requested total amount of splits. This function may generate more
     *     splits.
     * @param ringTokens list of all start tokens in Cassandra cluster. They have to be in ring
     *     order.
     * @return list containing at least {@code totalSplitCount} CassandraSplits.
     */
    public List<CassandraSplit> generateSplits(long totalSplitCount, List<BigInteger> ringTokens) {
        if (totalSplitCount == 1) {
            RingRange totalRingRange = RingRange.of(rangeMin, rangeMax);
            // needs to be mutable
            return Collections.singletonList(new CassandraSplit(Sets.newHashSet(totalRingRange)));
        }
        int tokenRangeCount = ringTokens.size();

        List<RingRange> ringRanges = new ArrayList<>();
        for (int i = 0; i < tokenRangeCount; i++) {
            BigInteger start = ringTokens.get(i);
            BigInteger stop = ringTokens.get((i + 1) % tokenRangeCount);

            if (isNotInRange(start) || isNotInRange(stop)) {
                throw new RuntimeException(
                        String.format(
                                "Tokens (%s,%s) not in range of %s", start, stop, partitioner));
            }
            if (start.equals(stop) && tokenRangeCount != 1) {
                throw new RuntimeException(
                        String.format(
                                "Tokens (%s,%s): two nodes have the same token", start, stop));
            }

            BigInteger rangeSize = stop.subtract(start);
            if (rangeSize.compareTo(BigInteger.ZERO) <= 0) {
                // wrap around case
                rangeSize = rangeSize.add(this.rangeSize);
            }

            // the below, in essence, does this:
            // splitCount = Maths.ceil((rangeSize / cluster range size) * totalSplitCount)
            BigInteger[] splitCountAndRemainder =
                    rangeSize
                            .multiply(BigInteger.valueOf(totalSplitCount))
                            .divideAndRemainder(this.rangeSize);

            int splitCount =
                    splitCountAndRemainder[0].intValue()
                            + (splitCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

            LOG.debug("Dividing token range [{},{}) into {} splits", start, stop, splitCount);

            // Make BigInteger list of all the endpoints for the splits, including both start and
            // stop
            List<BigInteger> endpointTokens = new ArrayList<>();
            for (int j = 0; j <= splitCount; j++) {
                BigInteger offset =
                        rangeSize
                                .multiply(BigInteger.valueOf(j))
                                .divide(BigInteger.valueOf(splitCount));
                BigInteger token = start.add(offset);
                if (token.compareTo(rangeMax) > 0) {
                    token = token.subtract(this.rangeSize);
                }
                // Long.MIN_VALUE is not a valid token and has to be silently incremented.
                // See https://issues.apache.org/jira/browse/CASSANDRA-14684
                endpointTokens.add(
                        token.equals(BigInteger.valueOf(Long.MIN_VALUE))
                                ? token.add(BigInteger.ONE)
                                : token);
            }

            // Append the ringRanges between the endpoints
            for (int j = 0; j < splitCount; j++) {
                ringRanges.add(RingRange.of(endpointTokens.get(j), endpointTokens.get(j + 1)));
                LOG.debug(
                        "Split #{}: [{},{})",
                        j + 1,
                        endpointTokens.get(j),
                        endpointTokens.get(j + 1));
            }
        }

        BigInteger total = BigInteger.ZERO;
        for (RingRange split : ringRanges) {
            BigInteger size = split.span(rangeSize);
            total = total.add(size);
        }
        if (!total.equals(rangeSize)) {
            throw new RuntimeException(
                    "Some tokens are missing from the splits. This should not happen.");
        }
        return coalesceRingRanges(getTargetSplitSize(totalSplitCount), ringRanges);
    }

    private boolean isNotInRange(BigInteger token) {
        return token.compareTo(rangeMin) < 0 || token.compareTo(rangeMax) > 0;
    }

    private List<CassandraSplit> coalesceRingRanges(
            BigInteger targetSplitSize, List<RingRange> ringRanges) {
        List<CassandraSplit> coalescedSplits = new ArrayList<>();
        List<RingRange> tokenRangesForCurrentSplit = new ArrayList<>();
        BigInteger tokenCount = BigInteger.ZERO;

        for (RingRange tokenRange : ringRanges) {
            if (tokenRange.span(rangeSize).add(tokenCount).compareTo(targetSplitSize) > 0
                    && !tokenRangesForCurrentSplit.isEmpty()) {
                // enough tokens in that segment
                LOG.debug(
                        "Got enough tokens for one split ({}) : {}",
                        tokenCount,
                        tokenRangesForCurrentSplit);
                coalescedSplits.add(new CassandraSplit(new HashSet<>(tokenRangesForCurrentSplit)));
                tokenRangesForCurrentSplit = new ArrayList<>();
                tokenCount = BigInteger.ZERO;
            }

            tokenCount = tokenCount.add(tokenRange.span(rangeSize));
            tokenRangesForCurrentSplit.add(tokenRange);
        }

        if (!tokenRangesForCurrentSplit.isEmpty()) {
            coalescedSplits.add(new CassandraSplit(new HashSet<>(tokenRangesForCurrentSplit)));
        }
        return coalescedSplits;
    }

    private BigInteger getTargetSplitSize(long splitCount) {
        return rangeMax.subtract(rangeMin).divide(BigInteger.valueOf(splitCount));
    }
}
