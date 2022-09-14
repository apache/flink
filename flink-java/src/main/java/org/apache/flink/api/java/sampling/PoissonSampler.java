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

package org.apache.flink.api.java.sampling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import org.apache.commons.math3.distribution.PoissonDistribution;

import java.util.Iterator;
import java.util.Random;

/**
 * A sampler implementation based on the Poisson Distribution. While sampling elements with fraction
 * and replacement, the selected number of each element follows a given poisson distribution.
 *
 * @param <T> The type of sample.
 * @see <a
 *     href="https://en.wikipedia.org/wiki/Poisson_distribution">https://en.wikipedia.org/wiki/Poisson_distribution</a>
 * @see <a
 *     href="http://erikerlandson.github.io/blog/2014/09/11/faster-random-samples-with-gap-sampling/">Gap
 *     Sampling</a>
 */
@Internal
public class PoissonSampler<T> extends RandomSampler<T> {

    private PoissonDistribution poissonDistribution;
    private final double fraction;
    private final Random random;

    // THRESHOLD is a tuning parameter for choosing sampling method according to the fraction.
    private static final double THRESHOLD = 0.4;

    /**
     * Create a poisson sampler which can sample elements with replacement.
     *
     * @param fraction The expected count of each element.
     * @param seed Random number generator seed for internal PoissonDistribution.
     */
    public PoissonSampler(double fraction, long seed) {
        Preconditions.checkArgument(fraction >= 0, "fraction should be positive.");
        this.fraction = fraction;
        if (this.fraction > 0) {
            this.poissonDistribution = new PoissonDistribution(fraction);
            this.poissonDistribution.reseedRandomGenerator(seed);
        }
        this.random = new XORShiftRandom(seed);
    }

    /**
     * Create a poisson sampler which can sample elements with replacement.
     *
     * @param fraction The expected count of each element.
     */
    public PoissonSampler(double fraction) {
        Preconditions.checkArgument(fraction >= 0, "fraction should be non-negative.");
        this.fraction = fraction;
        if (this.fraction > 0) {
            this.poissonDistribution = new PoissonDistribution(fraction);
        }
        this.random = new XORShiftRandom();
    }

    /**
     * Sample the input elements, for each input element, generate its count following a poisson
     * distribution.
     *
     * @param input Elements to be sampled.
     * @return The sampled result which is lazy computed upon input elements.
     */
    @Override
    public Iterator<T> sample(final Iterator<T> input) {
        if (fraction == 0) {
            return emptyIterable;
        }

        return new SampledIterator<T>() {
            T currentElement;
            int currentCount = 0;

            @Override
            public boolean hasNext() {
                if (currentCount > 0) {
                    return true;
                } else {
                    samplingProcess();
                    if (currentCount > 0) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public T next() {
                if (currentCount <= 0) {
                    samplingProcess();
                }
                currentCount--;
                return currentElement;
            }

            public int poisson_ge1(double p) {
                // sample 'k' from Poisson(p), conditioned to k >= 1.
                double q = Math.pow(Math.E, -p);
                // simulate a poisson trial such that k >= 1.
                double t = q + (1 - q) * random.nextDouble();
                int k = 1;
                // continue standard poisson generation trials.
                t = t * random.nextDouble();
                while (t > q) {
                    k++;
                    t = t * random.nextDouble();
                }
                return k;
            }

            private void skipGapElements(int num) {
                // skip the elements that occurrence number is zero.
                int elementCount = 0;
                while (input.hasNext() && elementCount < num) {
                    currentElement = input.next();
                    elementCount++;
                }
            }

            private void samplingProcess() {
                if (fraction <= THRESHOLD) {
                    double u = Math.max(random.nextDouble(), EPSILON);
                    int gap = (int) (Math.log(u) / -fraction);
                    skipGapElements(gap);
                    if (input.hasNext()) {
                        currentElement = input.next();
                        currentCount = poisson_ge1(fraction);
                    }
                } else {
                    while (input.hasNext()) {
                        currentElement = input.next();
                        currentCount = poissonDistribution.sample();
                        if (currentCount > 0) {
                            break;
                        }
                    }
                }
            }
        };
    }
}
