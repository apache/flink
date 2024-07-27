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
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.hadoop.mapred.JobConf;

/**
 * The factory class for {@link HiveParallelismInference} to support Hive source dynamic parallelism
 * inference.
 */
class HiveDynamicParallelismInferenceFactory implements HiveParallelismInference.Provider {

    private final ObjectPath tablePath;
    private final JobConf jobConf;
    private final int globalMaxParallelism;

    HiveDynamicParallelismInferenceFactory(
            ObjectPath tablePath, JobConf jobConf, int globalMaxParallelism) {
        this.tablePath = tablePath;
        this.jobConf = jobConf;
        this.globalMaxParallelism = globalMaxParallelism;
    }

    @Override
    public HiveParallelismInference create() {
        boolean inferEnabled =
                jobConf.getBoolean(
                        HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM.key(),
                        HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM.defaultValue());
        HiveOptions.InferMode inferMode =
                jobConf.getEnum(
                        HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MODE.key(),
                        HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MODE.defaultValue());
        // This logic should be fixed if config option `table.exec.hive.infer-source-parallelism`
        // is removed.
        boolean infer = inferEnabled && inferMode == HiveOptions.InferMode.DYNAMIC;
        int inferMaxParallelism =
                Math.min(
                        (int)
                                jobConf.getLong(
                                        HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX
                                                .key(),
                                        globalMaxParallelism),
                        globalMaxParallelism);
        int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        adjustInferMaxParallelism(jobConf, inferMaxParallelism);
        return new HiveParallelismInference(tablePath, infer, inferMaxParallelism, parallelism);
    }

    /**
     * Reset infer source max parallelism in jobConf, and {@link
     * HiveSourceFileEnumerator#createInputSplits} will infer InputSplits based on the
     * inferMaxParallelism.
     */
    private void adjustInferMaxParallelism(JobConf jobConf, int inferMaxParallelism) {
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX.key(),
                String.valueOf(inferMaxParallelism));
    }
}
