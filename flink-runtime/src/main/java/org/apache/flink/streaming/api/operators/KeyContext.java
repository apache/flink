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

/**
 * Interface for setting and querying the current key of keyed operations.
 *
 * <p>This is mainly used by the timer system to query the key when creating timers and to set the
 * correct key context when firing a timer.
 */
public interface KeyContext {

    void setCurrentKey(Object key);

    /**
     * Moves to the next key of a key-sorted input, e.g. when the next record of a sorted input in
     * BATCH execution belongs to a new key. Unlike {@link #setCurrentKey}, it must never go back to
     * a key that was current before. In BATCH execution keys may only change this way.
     */
    default void moveToNextKey(Object key) {
        setCurrentKey(key);
    }

    Object getCurrentKey();
}
