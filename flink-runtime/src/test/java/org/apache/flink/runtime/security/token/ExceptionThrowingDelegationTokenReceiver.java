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

import org.apache.flink.configuration.Configuration;

/**
 * An example implementation of {@link DelegationTokenReceiver} which throws exception when enabled.
 */
public class ExceptionThrowingDelegationTokenReceiver implements DelegationTokenReceiver {

    public static volatile boolean throwInInit = false;
    public static volatile boolean throwInUsage = false;
    public static volatile boolean constructed = false;

    public static void reset() {
        throwInInit = false;
        throwInUsage = false;
        constructed = false;
    }

    public ExceptionThrowingDelegationTokenReceiver() {
        constructed = true;
    }

    @Override
    public String serviceName() {
        return "throw";
    }

    @Override
    public void init(Configuration configuration) {
        if (throwInInit) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        if (throwInUsage) {
            throw new IllegalArgumentException();
        }
    }
}
