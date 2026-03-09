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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test scalar functions for verifying Common Sub-expression Elimination (CSE).
 *
 * <p>These UDFs maintain static call counters so that tests can verify that duplicate
 * sub-expressions are only evaluated once when CSE is enabled. For example, given:
 *
 * <pre>{@code
 * SELECT testcse2(testcse(sid)), testcse3(testcse(sid)), testcse4(testcse(sid))
 * FROM input
 * }</pre>
 *
 * <p>Without CSE, {@code testcse(sid)} is called 3 times per row. With CSE, it is called only once
 * and the result is reused, leading to significant performance improvements for expensive UDFs.
 */
public class CseTestFunctions {

    /** Reset all call counters. Should be called before each test. */
    public static void resetCounters() {
        TestCseFunc.CALL_COUNT.set(0);
        TestCse2Func.CALL_COUNT.set(0);
        TestCse3Func.CALL_COUNT.set(0);
        TestCse4Func.CALL_COUNT.set(0);
    }

    /**
     * A scalar function that simulates an expensive computation (the common sub-expression).
     * Maintains a call counter for verification.
     */
    public static class TestCseFunc extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

        public String eval(String input) {
            CALL_COUNT.incrementAndGet();
            // Simulate some computation
            return input != null ? input.toUpperCase() : null;
        }
    }

    /** Wrapper function 2: appends "_2" to its input. */
    public static class TestCse2Func extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

        public String eval(String input) {
            CALL_COUNT.incrementAndGet();
            return input != null ? input + "_2" : null;
        }
    }

    /** Wrapper function 3: appends "_3" to its input. */
    public static class TestCse3Func extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

        public String eval(String input) {
            CALL_COUNT.incrementAndGet();
            return input != null ? input + "_3" : null;
        }
    }

    /** Wrapper function 4: appends "_4" to its input. */
    public static class TestCse4Func extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

        public String eval(String input) {
            CALL_COUNT.incrementAndGet();
            return input != null ? input + "_4" : null;
        }
    }

    /**
     * A scalar function that simulates an expensive computation with a deliberate delay. Used in
     * performance benchmarks to demonstrate CSE speedup.
     */
    public static class SlowTestCseFunc extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

        public String eval(String input) throws InterruptedException {
            CALL_COUNT.incrementAndGet();
            Thread.sleep(20); // Simulate expensive I/O or computation
            return input != null ? input.toUpperCase() : null;
        }
    }
}
