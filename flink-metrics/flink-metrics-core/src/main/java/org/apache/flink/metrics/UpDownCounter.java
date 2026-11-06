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

package org.apache.flink.metrics;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@link Counter} whose count may both increase and decrease.
 *
 * <p>{@link Counter#dec()} / {@link Counter#dec(long)} are deprecated on the general {@link
 * Counter} contract because flink system counters are effectively monotonic (see {@link
 * MonotonicCounter}), Use {@code UpDownCounter} to declare a counter whose value is genuinely
 * expected to go up and down. decrementing remains a fully supported, non-deprecated operation
 * here.
 */
@PublicEvolving
public interface UpDownCounter extends Counter {

    /**
     * {@inheritDoc}
     *
     * <p>Unlike the general {@link Counter} contract, decrementing is fully supported.
     */
    @Override
    void dec();

    /**
     * {@inheritDoc}
     *
     * <p>Unlike the general {@link Counter} contract, decrementing is fully supported.
     */
    @Override
    void dec(long n);
}
