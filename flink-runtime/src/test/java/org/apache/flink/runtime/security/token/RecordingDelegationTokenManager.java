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

package org.apache.flink.runtime.security.token;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link DelegationTokenManager} which does nothing but remember how it was used. */
public final class RecordingDelegationTokenManager extends NoOpDelegationTokenManager {

    private final AtomicInteger startCalls = new AtomicInteger();
    private final AtomicInteger stopCalls = new AtomicInteger();
    private final List<Listener> attachedListeners = new CopyOnWriteArrayList<>();
    private final List<Listener> detachedListeners = new CopyOnWriteArrayList<>();

    public int startCalls() {
        return startCalls.get();
    }

    public int stopCalls() {
        return stopCalls.get();
    }

    /** The listeners passed to {@link #attachListener(Listener)} in the order of the calls. */
    public List<Listener> attachedListeners() {
        return attachedListeners;
    }

    /** The listeners passed to {@link #detachListener(Listener)} in the order of the calls. */
    public List<Listener> detachedListeners() {
        return detachedListeners;
    }

    @Override
    public void start() {
        startCalls.incrementAndGet();
    }

    @Override
    public void attachListener(Listener listener) {
        attachedListeners.add(listener);
    }

    @Override
    public void detachListener(Listener listener) {
        detachedListeners.add(listener);
    }

    @Override
    public void stop() {
        stopCalls.incrementAndGet();
    }
}
