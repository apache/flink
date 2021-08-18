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

package org.apache.flink.streaming.runtime.tasks;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** A utility class to record the number of calls for each phase. */
class LifeCycleMonitor implements Serializable {

    /** The lifecycle of the one-input operator. */
    public enum LifeCyclePhase {
        OPEN,
        INITIALIZE_STATE,
        PROCESS_ELEMENT,
        PREPARE_SNAPSHOT_PRE_BARRIER,
        SNAPSHOT_STATE,
        NOTIFY_CHECKPOINT_COMPLETE,
        NOTIFY_CHECKPOINT_ABORT,
        FINISH,
        CLOSE
    }

    private final Map<LifeCyclePhase, Integer> callTimes = new HashMap<>();

    public void incrementCallTime(LifeCyclePhase phase) {
        callTimes.compute(phase, (k, v) -> v == null ? 1 : v + 1);
    }

    public void assertCallTimes(int expectedTimes, LifeCyclePhase... phases) {
        for (LifeCyclePhase phase : phases) {
            assertEquals(
                    String.format("The phase %s has unexpected call times", phase),
                    expectedTimes,
                    callTimes.getOrDefault(phase, 0).intValue());
        }
    }
}
