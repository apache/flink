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

package org.apache.flink.util;

import org.apache.flink.annotation.Public;

import java.util.Random;

/**
 * Implement a random number generator based on the XORShift algorithm discovered by George
 * Marsaglia. This RNG is observed 4.5 times faster than {@link java.util.Random} in benchmark, with
 * the cost that abandon thread-safety. So it's recommended to create a new {@link XORShiftRandom}
 * for each thread.
 *
 * @see <a href="http://www.jstatsoft.org/v08/i14/paper">XORShift Algorithm Paper</a>
 */
@Public
public class XORShiftRandom extends Random {

    private static final long serialVersionUID = -825722456120842841L;
    private long seed;

    public XORShiftRandom() {
        this(System.nanoTime());
    }

    public XORShiftRandom(long input) {
        super(input);
        this.seed = MathUtils.murmurHash((int) input) ^ MathUtils.murmurHash((int) (input >>> 32));
    }

    /**
     * All other methods like nextInt()/nextDouble()... depends on this, so we just need to
     * overwrite this.
     *
     * @param bits Random bits
     * @return The next pseudorandom value from this random number generator's sequence
     */
    @Override
    public int next(int bits) {
        long nextSeed = seed ^ (seed << 21);
        nextSeed ^= (nextSeed >>> 35);
        nextSeed ^= (nextSeed << 4);
        seed = nextSeed;
        return (int) (nextSeed & ((1L << bits) - 1));
    }
}
