/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.MemorySegment;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * BloomFilter is a probabilistic data structure for set membership check. BloomFilters are highly
 * space efficient when compared to using a HashSet. Because of the probabilistic nature of bloom
 * filter false positive (element not present in bloom filter but test() says true) are possible but
 * false negatives are not possible (if element is present then test() will never say false). The
 * false positive probability is configurable depending on which storage requirement may increase or
 * decrease. Lower the false positive probability greater is the space requirement. Bloom filters
 * are sensitive to number of elements that will be inserted in the bloom filter. During the
 * creation of bloom filter expected number of entries must be specified. If the number of
 * insertions exceed the specified initial number of entries then false positive probability will
 * increase accordingly.
 *
 * <p>Internally, this implementation of bloom filter uses MemorySegment to store BitSet,
 * BloomFilter and BitSet are designed to be able to switch between different MemorySegments, so
 * that Flink can share the same BloomFilter/BitSet object instance for different bloom filters.
 *
 * <p>Part of this class refers to the implementation from Apache Hive project
 * https://github.com/apache/hive/blob/master/common/src/java/org/apache/hive/common/util/BloomFilter.java
 */
public class BloomFilter {

    protected BitSet bitSet;
    protected int expectedEntries;
    protected int numHashFunctions;

    public BloomFilter(int expectedEntries, int byteSize) {
        checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
        this.expectedEntries = expectedEntries;
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, byteSize << 3);
        this.bitSet = new BitSet(byteSize);
    }

    public void setBitsLocation(MemorySegment memorySegment, int offset) {
        this.bitSet.setMemorySegment(memorySegment, offset);
    }

    /**
     * Compute optimal bits number with given input entries and expected false positive probability.
     *
     * @param inputEntries
     * @param fpp
     * @return optimal bits number
     */
    public static int optimalNumOfBits(long inputEntries, double fpp) {
        int numBits = (int) (-inputEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        return numBits;
    }

    /**
     * Compute the false positive probability based on given input entries and bits size. Note: this
     * is just the math expected value, you should not expect the fpp in real case would under the
     * return value for certain.
     *
     * @param inputEntries
     * @param bitSize
     * @return
     */
    public static double estimateFalsePositiveProbability(long inputEntries, int bitSize) {
        int numFunction = optimalNumOfHashFunctions(inputEntries, bitSize);
        double p = Math.pow(Math.E, -(double) numFunction * inputEntries / bitSize);
        double estimatedFPP = Math.pow(1 - p, numFunction);
        return estimatedFPP;
    }

    /**
     * compute the optimal hash function number with given input entries and bits size, which would
     * make the false positive probability lowest.
     *
     * @param expectEntries
     * @param bitSize
     * @return hash function number
     */
    static int optimalNumOfHashFunctions(long expectEntries, long bitSize) {
        return Math.max(1, (int) Math.round((double) bitSize / expectEntries * Math.log(2)));
    }

    public void addHash(int hash32) {
        int hash1 = hash32;
        int hash2 = hash32 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            bitSet.set(pos);
        }
    }

    public boolean testHash(int hash32) {
        int hash1 = hash32;
        int hash2 = hash32 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public void reset() {
        this.bitSet.clear();
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder();
        output.append("BloomFilter:\n");
        output.append("\thash function number:").append(numHashFunctions).append("\n");
        output.append(bitSet);
        return output.toString();
    }
}
