/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Sources that implement this interface will dynamically infer the source’s parallelism when it is
 * unspecified. It will be invoked during the Flink runtime before the source vertex is scheduled.
 *
 * <p>The implementations typically work together with the {@link Source} and are currently only
 * effective for batch jobs that use the adaptive batch scheduler.
 */
@PublicEvolving
public interface DynamicParallelismInference {
    /** A context that provides dynamic parallelism decision infos. */
    @PublicEvolving
    interface Context {
        /**
         * Get the dynamic filtering info of the source vertex.
         *
         * @return the dynamic filter instance.
         */
        Optional<DynamicFilteringInfo> getDynamicFilteringInfo();

        /**
         * Get the upper bound for the inferred parallelism.
         *
         * @return the upper bound for the inferred parallelism.
         */
        int getParallelismInferenceUpperBound();

        /**
         * Get the average size of data volume (in bytes) to expect each task instance to process.
         *
         * @return the data volume per task in bytes.
         */
        long getDataVolumePerTask();
    }

    /**
     * The method is invoked on the master (JobManager) before the initialization of the source
     * vertex.
     *
     * @param context The context to get dynamic parallelism decision infos.
     */
    int inferParallelism(Context context);
}
