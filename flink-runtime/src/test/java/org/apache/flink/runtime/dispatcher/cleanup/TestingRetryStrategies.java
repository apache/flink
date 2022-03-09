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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.time.Duration;

/** {@code TestingRetryStrategies} collects common {@link RetryStrategy} variants. */
public class TestingRetryStrategies {

    private TestingRetryStrategies() {
        // utility class shouldn't be instantiated
    }

    private static final Duration TESTING_DEFAULT_RETRY_DELAY = Duration.ofMillis(10);

    public static final RetryStrategy NO_RETRY_STRATEGY = new FixedRetryStrategy(0, Duration.ZERO);

    public static RetryStrategy createWithNumberOfRetries(int retryCount) {
        return new FixedRetryStrategy(retryCount, TESTING_DEFAULT_RETRY_DELAY);
    }
}
