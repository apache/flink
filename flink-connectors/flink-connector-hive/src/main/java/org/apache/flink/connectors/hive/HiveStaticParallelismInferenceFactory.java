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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;

/**
 * The factory class for {@link HiveParallelismInference} to support Hive source static parallelism
 * inference.
 */
class HiveStaticParallelismInferenceFactory implements HiveParallelismInference.Provider {

    private final ObjectPath tablePath;
    private final ReadableConfig flinkConf;

    HiveStaticParallelismInferenceFactory(ObjectPath tablePath, ReadableConfig flinkConf) {
        this.tablePath = tablePath;
        this.flinkConf = flinkConf;
    }

    @Override
    public HiveParallelismInference create() {
        boolean inferEnabled = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM);
        HiveOptions.InferMode inferMode =
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MODE);
        // This logic should be fixed if config option `table.exec.hive.infer-source-parallelism`
        // is removed.
        boolean infer = inferEnabled && inferMode == HiveOptions.InferMode.STATIC;
        int inferMaxParallelism =
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX);
        Preconditions.checkArgument(
                inferMaxParallelism >= 1,
                HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX.key()
                        + " cannot be less than 1");
        int parallelism =
                flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
        // Keeping the parallelism unset is a prerequisite for dynamic parallelism inference.
        if (inferEnabled && inferMode == HiveOptions.InferMode.DYNAMIC) {
            parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        }

        return new HiveParallelismInference(tablePath, infer, inferMaxParallelism, parallelism);
    }
}
