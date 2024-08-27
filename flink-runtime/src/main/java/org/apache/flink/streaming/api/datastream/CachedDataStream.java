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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.CacheTransformation;

/**
 * {@link CachedDataStream} represents a {@link DataStream} whose intermediate result will be cached
 * at the first time when it is computed. And the cached intermediate result can be used in later
 * job that using the same {@link CachedDataStream} to avoid re-computing the intermediate result.
 *
 * @param <T> The type of the elements in this stream.
 */
@PublicEvolving
public class CachedDataStream<T> extends DataStream<T> {
    /**
     * Create a new {@link CachedDataStream} in the given execution environment that wrap the given
     * physical transformation to indicates that the transformation should be cached.
     *
     * @param environment The StreamExecutionEnvironment
     * @param transformation The physical transformation whose intermediate result should be cached.
     */
    public CachedDataStream(
            StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(
                environment,
                new CacheTransformation<>(
                        transformation, String.format("Cache: %s", transformation.getName())));

        final CacheTransformation<T> t = (CacheTransformation<T>) this.getTransformation();
        environment.registerCacheTransformation(t.getDatasetId(), t);
    }

    /**
     * Invalidate the cache intermediate result of this DataStream to release the physical
     * resources. Users are not required to invoke this method to release physical resources unless
     * they want to. Cache will be recreated if it is used after invalidated.
     */
    public void invalidate() throws Exception {
        final CacheTransformation<T> t = (CacheTransformation<T>) this.getTransformation();
        environment.invalidateClusterDataset(t.getDatasetId());
    }
}
