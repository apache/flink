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

import java.util.ArrayList;
import java.util.List;

/**
 * This base class handles the task of dividing the requested work into the appropriate number of
 * blocks of near-equal size.
 *
 * @param <T> the type of the {@code RandomGenerator}
 */
public abstract class GeneratorFactoryBase<T extends RandomGenerator>
        implements RandomGenerableFactory<T> {

    // A large computation will run in parallel but blocks are generated on
    // and distributed from a single node. This limit should be greater
    // than the maximum expected parallelism.
    public static final int MAXIMUM_BLOCK_COUNT = 1 << 15;

    // This should be sufficiently large relative to the cost of instantiating
    // and initializing the random generator and sufficiently small relative to
    // the cost of generating random values.
    protected abstract int getMinimumCyclesPerBlock();

    protected abstract RandomGenerable<T> next();

    @Override
    public List<BlockInfo<T>> getRandomGenerables(long elementCount, int cyclesPerElement) {
        long cycles = elementCount * cyclesPerElement;
        int blockCount =
                Math.min(
                        (int) Math.ceil(cycles / (float) getMinimumCyclesPerBlock()),
                        MAXIMUM_BLOCK_COUNT);

        long elementsPerBlock = elementCount / blockCount;
        long elementRemainder = elementCount % blockCount;

        List<BlockInfo<T>> blocks = new ArrayList<>(blockCount);
        long blockStart = 0;

        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            if (blockIndex == blockCount - elementRemainder) {
                elementsPerBlock++;
            }

            RandomGenerable<T> randomGenerable = next();

            blocks.add(
                    new BlockInfo<>(
                            randomGenerable, blockIndex, blockCount, blockStart, elementsPerBlock));

            blockStart += elementsPerBlock;
        }

        return blocks;
    }
}
