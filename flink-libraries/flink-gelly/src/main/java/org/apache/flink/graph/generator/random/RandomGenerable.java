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
 * A RandomGenerable provides deferred instantiation and initialization of a RandomGenerator. This
 * allows pre-processing or discovery to be distributed and performed in parallel by Flink tasks.
 *
 * <p>A distributed PRNG is described by Matsumoto and Takuji in "Dynamic Creation of Pseudorandom
 * Number Generators".
 *
 * @param <T> the type of the {@code RandomGenerator}
 */
public interface RandomGenerable<T extends RandomGenerator> {

    /**
     * Returns an initialized {@link RandomGenerator}.
     *
     * @return a {@code RandomGenerator} of type {@code T}
     */
    T generator();
}
