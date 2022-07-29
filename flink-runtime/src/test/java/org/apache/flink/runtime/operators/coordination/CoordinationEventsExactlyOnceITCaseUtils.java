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

package org.apache.flink.runtime.operators.coordination;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utility classes and methods shared in integration test cases about the exactly-once semantics of
 * the delivery of operator events.
 */
public class CoordinationEventsExactlyOnceITCaseUtils {

    /**
     * Checks whether the integer list, {@code ints}, contains exactly {@code length} numbers,
     * ranging from 0 to {@code length} - 1.
     */
    public static void checkListContainsSequence(List<Integer> ints, int length) {
        Integer[] expected = new Integer[length];
        for (int i = 0; i < length; i++) {
            expected[i] = i;
        }
        assertThat(ints).containsExactly(expected);
    }

    /**
     * An operator event to notify the coordinator that the test subtask is ready to accept events.
     */
    public static final class StartEvent implements OperatorEvent {

        /**
         * The last integer value the subtask has received from the coordinator and stored in
         * snapshot, or -1 if the subtask has not completed any checkpoint yet.
         */
        public final int lastValue;

        public StartEvent(int lastValue) {
            this.lastValue = lastValue;
        }
    }

    public static final class EndEvent implements OperatorEvent {}

    public static final class IntegerEvent implements OperatorEvent {

        public final int value;

        public IntegerEvent(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "IntegerEvent " + value;
        }
    }

    // ------------------------------------------------------------------------
    //  dedicated class to hold the "test script"
    // ------------------------------------------------------------------------

    public static final class TestScript {

        private static final Map<String, TestScript> MAP_FOR_OPERATOR = new HashMap<>();

        public static TestScript getForOperator(String operatorName) {
            return MAP_FOR_OPERATOR.computeIfAbsent(operatorName, (key) -> new TestScript());
        }

        public static void reset() {
            MAP_FOR_OPERATOR.clear();
        }

        private final Collection<CountDownLatch> recoveredTaskRunning = new ArrayList<>();
        private boolean failedBefore;

        public void recordHasFailed() {
            this.failedBefore = true;
        }

        public boolean hasAlreadyFailed() {
            return failedBefore;
        }

        void registerHookToNotifyAfterTaskRecovered(CountDownLatch latch) {
            synchronized (recoveredTaskRunning) {
                recoveredTaskRunning.add(latch);
            }
        }

        void signalRecoveredTaskReady() {
            // We complete all latches that were registered. We may need to complete
            // multiple ones here, because it can happen that after a previous failure, the next
            // executions fails immediately again, before even registering at the coordinator.
            // in that case, we have multiple latches from multiple failure notifications waiting
            // to be completed.
            synchronized (recoveredTaskRunning) {
                for (CountDownLatch latch : recoveredTaskRunning) {
                    latch.countDown();
                }
                recoveredTaskRunning.clear();
            }
        }
    }
}
