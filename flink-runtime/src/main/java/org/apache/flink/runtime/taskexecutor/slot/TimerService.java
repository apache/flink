/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.slot;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Service to register timeouts for a given key. The timeouts are identified by a ticket so that
 * newly registered timeouts for the same key can be distinguished from older timeouts.
 *
 * @param <K> Type of the key
 */
public interface TimerService<K> {

    /**
     * Starts this timer service.
     *
     * @param timeoutListener listener for timeouts that have fired
     */
    void start(TimeoutListener<K> timeoutListener);

    /** Stops this timer service. */
    void stop();

    /**
     * Register a timeout for the given key which shall occur in the given delay.
     *
     * @param key for which to register the timeout
     * @param delay until the timeout
     * @param unit of the timeout delay
     */
    void registerTimeout(K key, long delay, TimeUnit unit);

    /**
     * Unregister the timeout for the given key.
     *
     * @param key for which to unregister the timeout
     */
    void unregisterTimeout(K key);

    /**
     * Check whether the timeout for the given key and ticket is still valid (not yet unregistered
     * and not yet overwritten).
     *
     * @param key for which to check the timeout
     * @param ticket of the timeout
     * @return True if the timeout ticket is still valid; otherwise false
     */
    boolean isValid(K key, UUID ticket);
}
