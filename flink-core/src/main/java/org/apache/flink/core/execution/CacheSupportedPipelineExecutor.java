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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The pipeline executor that support caching intermediate dataset. */
@Internal
public interface CacheSupportedPipelineExecutor extends PipelineExecutor {

    /**
     * Return a set of ids of the completed cluster dataset.
     *
     * @param configuration the {@link Configuration} with the required parameters
     * @param userCodeClassloader the {@link ClassLoader} to deserialize usercode
     * @return A set of ids of the completely cached intermediate dataset.
     */
    CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds(
            final Configuration configuration, final ClassLoader userCodeClassloader)
            throws Exception;

    /**
     * Invalidate the cluster dataset with the given id.
     *
     * @param clusterDatasetId id of the cluster dataset to be invalidated.
     * @param configuration the {@link Configuration} with the required parameters
     * @param userCodeClassloader the {@link ClassLoader} to deserialize usercode
     * @return Future which will be completed when the cached dataset is invalidated.
     */
    CompletableFuture<Void> invalidateClusterDataset(
            AbstractID clusterDatasetId,
            final Configuration configuration,
            final ClassLoader userCodeClassloader)
            throws Exception;
}
