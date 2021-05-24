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

package org.apache.flink.graph.generator.random;

import org.apache.commons.math3.random.RandomGenerator;

/**
 * Defines a source of randomness and a unit of work.
 *
 * @param <T> the type of the {@code RandomGenerator}
 */
public class BlockInfo<T extends RandomGenerator> {

    private final RandomGenerable<T> randomGenerable;

    private final int blockIndex;

    private final int blockCount;

    private final long firstElement;

    private final long elementCount;

    public BlockInfo(
            RandomGenerable<T> randomGenerable,
            int blockIndex,
            int blockCount,
            long firstElement,
            long elementCount) {
        this.randomGenerable = randomGenerable;
        this.blockIndex = blockIndex;
        this.blockCount = blockCount;
        this.firstElement = firstElement;
        this.elementCount = elementCount;
    }

    /** @return the source of randomness */
    public RandomGenerable<T> getRandomGenerable() {
        return randomGenerable;
    }

    /** @return the index of this block within the list of blocks */
    public int getBlockIndex() {
        return blockIndex;
    }

    /** @return the total number of blocks */
    public int getBlockCount() {
        return blockCount;
    }

    /** @return the index of the first element in this block */
    public long getFirstElement() {
        return firstElement;
    }

    /** @return the total number of elements across all blocks */
    public long getElementCount() {
        return elementCount;
    }
}
