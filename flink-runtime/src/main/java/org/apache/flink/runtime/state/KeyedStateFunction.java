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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;

/**
 * A function to be applied to all keyed states.
 *
 * @param <K> The type of key.
 * @param <S> The type of state.
 */
@FunctionalInterface
public interface KeyedStateFunction<K, S extends State> {

    /**
     * The actual method to be applied on each of the states.
     *
     * @param key the key whose state is being processed.
     * @param state the state associated with the aforementioned key.
     */
    void process(K key, S state) throws Exception;
}
