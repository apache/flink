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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

/**
 * This interface is used to optimize the calls of {@link Input#setKeyContextElement}, {@link
 * StreamOperator#setKeyContextElement1} and {@link StreamOperator#setKeyContextElement2}. We can
 * decide(at the inputs/operators initialization) whether to omit the calls of
 * "setKeyContextElement" according to the return value of {@link #hasKeyContext}. In this way, we
 * can omit the calls of "setKeyContextElement" for inputs/operators that don't have "KeyContext".
 *
 * <p>All inputs/operators that want to optimize the "setKeyContextElement" calls should implement
 * this interface.
 */
@Internal
public interface KeyContextHandler {

    /**
     * Whether the {@link Input} has "KeyContext". If false, we can omit the call of {@link
     * Input#setKeyContextElement} for each record.
     *
     * @return True if the {@link Input} has "KeyContext", false otherwise.
     */
    default boolean hasKeyContext() {
        return hasKeyContext1();
    }

    /**
     * Whether the first input of {@link StreamOperator} has "KeyContext". If false, we can omit the
     * call of {@link StreamOperator#setKeyContextElement1} for each record arrived on the first
     * input.
     *
     * @return True if the first input has "KeyContext", false otherwise.
     */
    default boolean hasKeyContext1() {
        return true;
    }

    /**
     * Whether the second input of {@link StreamOperator} has "KeyContext". If false, we can omit
     * the call of {@link StreamOperator#setKeyContextElement1} for each record arrived on the
     * second input.
     *
     * @return True if the second input has "KeyContext", false otherwise.
     */
    default boolean hasKeyContext2() {
        return true;
    }
}
