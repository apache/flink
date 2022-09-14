/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.threadinfo;

import javax.annotation.Nonnegative;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A wrapper for parameters of a thread info sampling request. */
public class ThreadInfoSamplesRequest implements Serializable {
    private static final long serialVersionUID = -4360206136386773663L;

    private final int requestId;
    private final int numSubSamples;
    private final Duration delayBetweenSamples;
    private final int maxStackTraceDepth;

    /**
     * @param requestId ID of the sampling request.
     * @param numSamples The number of samples.
     * @param delayBetweenSamples The time to wait between taking samples.
     * @param maxStackTraceDepth The maximum depth of the returned stack traces.
     */
    public ThreadInfoSamplesRequest(
            int requestId,
            @Nonnegative int numSamples,
            Duration delayBetweenSamples,
            @Nonnegative int maxStackTraceDepth) {
        checkArgument(numSamples > 0, "numSamples must be positive");
        checkArgument(maxStackTraceDepth > 0, "maxStackTraceDepth must be positive");
        checkNotNull(delayBetweenSamples, "delayBetweenSamples must not be null");

        this.requestId = requestId;
        this.numSubSamples = numSamples;
        this.delayBetweenSamples = delayBetweenSamples;
        this.maxStackTraceDepth = maxStackTraceDepth;
    }

    /**
     * Returns the ID of the sampling request.
     *
     * @return ID of the request.
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Returns the number of samples that are requested to be collected.
     *
     * @return the number of requested samples.
     */
    public int getNumSamples() {
        return numSubSamples;
    }

    /**
     * Returns the configured delay between the individual samples.
     *
     * @return the delay between the individual samples.
     */
    public Duration getDelayBetweenSamples() {
        return delayBetweenSamples;
    }

    /**
     * Returns the configured maximum depth of the collected stack traces.
     *
     * @return the maximum depth of the collected stack traces.
     */
    public int getMaxStackTraceDepth() {
        return maxStackTraceDepth;
    }
}
