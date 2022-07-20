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

package org.apache.flink.runtime.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility exists to bridge between the visibility of the {@code currentMainThread} field in
 * the {@link RpcEndpoint}.
 *
 * <p>The {@code currentMainThread} can be hidden from {@code RpcEndpoint} implementations and only
 * be accessed via this utility from other packages.
 */
public final class MainThreadValidatorUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MainThreadValidatorUtil.class);

    private final RpcEndpoint endpoint;

    public MainThreadValidatorUtil(RpcEndpoint endpoint) {
        this.endpoint = checkNotNull(endpoint);
    }

    public void enterMainThread() {
        assert (endpoint.currentMainThread.compareAndSet(null, Thread.currentThread()))
                : "The RpcEndpoint has concurrent access from " + endpoint.currentMainThread.get();
    }

    public void exitMainThread() {
        assert (endpoint.currentMainThread.compareAndSet(Thread.currentThread(), null))
                : "The RpcEndpoint has concurrent access from " + endpoint.currentMainThread.get();
    }

    /**
     * Returns true iff the current thread is equals to the provided expected thread and logs
     * violations.
     *
     * @param expected the expected main thread.
     * @return true iff the current thread is equals to the provided expected thread.
     */
    public static boolean isRunningInExpectedThread(@Nullable Thread expected) {
        Thread actual = Thread.currentThread();
        if (expected != actual) {

            String violationMsg =
                    "Violation of main thread constraint detected: expected <"
                            + expected
                            + "> but running in <"
                            + actual
                            + ">.";

            LOG.warn(violationMsg, new Exception(violationMsg));

            return false;
        }

        return true;
    }
}
