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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeService;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionKeyedStateBackend;

/** Utilities used by Python operators. */
@Internal
public class PythonOperatorUtils {

    /** Set the current key for streaming operator. */
    public static <K> void setCurrentKeyForStreaming(
            KeyedStateBackend<K> stateBackend, K currentKey) {
        if (!inBatchExecutionMode(stateBackend)) {
            stateBackend.setCurrentKey(currentKey);
        }
    }

    /** Set the current key for the timer service. */
    public static <K, N> void setCurrentKeyForTimerService(
            InternalTimerService<N> internalTimerService, K currentKey) throws Exception {
        if (internalTimerService instanceof BatchExecutionInternalTimeService) {
            ((BatchExecutionInternalTimeService<K, N>) internalTimerService)
                    .setCurrentKey(currentKey);
        }
    }

    public static <K> boolean inBatchExecutionMode(KeyedStateBackend<K> stateBackend) {
        return stateBackend instanceof BatchExecutionKeyedStateBackend;
    }
}
