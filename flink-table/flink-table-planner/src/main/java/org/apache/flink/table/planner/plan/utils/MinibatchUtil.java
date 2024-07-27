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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.trigger.CoBundleTrigger;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountCoBundleTrigger;

/** Utility class for mini-batch related config. */
public class MinibatchUtil {

    /**
     * Check if MiniBatch is enabled.
     *
     * @param config config
     * @return true if MiniBatch enabled else false.
     */
    public static boolean isMiniBatchEnabled(ReadableConfig config) {
        return config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    }

    /**
     * Creates a MiniBatch trigger depends on the config for one input.
     *
     * @param config config
     * @return MiniBatch trigger
     */
    public static CountBundleTrigger<RowData> createMiniBatchTrigger(ReadableConfig config) {
        long size = miniBatchSize(config);
        return new CountBundleTrigger<>(size);
    }

    /**
     * Creates a MiniBatch trigger depends on the config for two input.
     *
     * @param config config
     * @return MiniBatch trigger
     */
    public static CoBundleTrigger<RowData, RowData> createMiniBatchCoTrigger(
            ReadableConfig config) {
        long size = miniBatchSize(config);
        return new CountCoBundleTrigger<>(size);
    }

    /**
     * Returns the mini batch size for given config and mixed mode flag, considering fallback logic.
     *
     * @param config config
     * @return mini batch size
     */
    public static long miniBatchSize(ReadableConfig config) {
        long size = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
        if (size <= 0) {
            throw new IllegalArgumentException(
                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE + " must be > 0.");
        }
        return size;
    }
}
