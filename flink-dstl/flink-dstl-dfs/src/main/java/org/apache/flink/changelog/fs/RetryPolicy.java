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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Retry policy to use by {@link RetryingExecutor}. */
@Internal
public interface RetryPolicy {
    static RetryPolicy fromConfig(ReadableConfig config) {
        switch (config.get(FsStateChangelogOptions.RETRY_POLICY)) {
            case "fixed":
                return fixed(
                        config.get(FsStateChangelogOptions.RETRY_MAX_ATTEMPTS),
                        config.get(FsStateChangelogOptions.UPLOAD_TIMEOUT).toMillis(),
                        config.get(FsStateChangelogOptions.RETRY_DELAY_AFTER_FAILURE).toMillis());
            case "none":
                return NONE;
            default:
                throw new IllegalConfigurationException(
                        "Unknown retry policy: "
                                + config.get(FsStateChangelogOptions.RETRY_POLICY));
        }
    }

    /** @return timeout in millis. Zero or negative means no timeout. */
    long timeoutFor(int attempt);

    /**
     * @return delay in millis before the next attempt. Negative means no retry, zero means no
     *     delay.
     */
    long retryAfter(int failedAttempt, Exception exception);

    RetryPolicy NONE =
            new RetryPolicy() {
                @Override
                public long timeoutFor(int attempt) {
                    return -1L;
                }

                @Override
                public long retryAfter(int failedAttempt, Exception exception) {
                    return -1L;
                }

                public String toString() {
                    return "none";
                }
            };

    static RetryPolicy fixed(int maxAttempts, long timeout, long delayAfterFailure) {
        return new FixedRetryPolicy(maxAttempts, timeout, delayAfterFailure);
    }

    /** {@link RetryPolicy} with fixed timeout, delay and max attempts. */
    class FixedRetryPolicy implements RetryPolicy {

        private final long timeout;
        private final int maxAttempts;
        private final long delayAfterFailure;

        FixedRetryPolicy(int maxAttempts, long timeout, long delayAfterFailure) {
            this.maxAttempts = maxAttempts;
            this.timeout = timeout;
            this.delayAfterFailure = delayAfterFailure;
        }

        @Override
        public long timeoutFor(int attempt) {
            return timeout;
        }

        @Override
        public long retryAfter(int attempt, Exception exception) {
            if (attempt >= maxAttempts) {
                return -1L;
            } else if (exception instanceof TimeoutException) {
                return 0L;
            } else if (exception instanceof IOException) {
                return delayAfterFailure;
            } else {
                return -1L;
            }
        }

        @Override
        public String toString() {
            return "timeout="
                    + timeout
                    + ", maxAttempts="
                    + maxAttempts
                    + ", delay="
                    + delayAfterFailure;
        }
    }
}
