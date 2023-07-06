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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * When in batch mode, the {@link CacheTransformation} represents the intermediate result of the
 * upper stream should be cached when it is computed at the first time. And it consumes the cached
 * intermediate result in later jobs. In stream mode, it has no affect.
 *
 * @param <T> The type of the elements in the cache intermediate result.
 */
@Internal
public class CacheTransformation<T> extends Transformation<T> {
    private final Transformation<T> transformationToCache;
    private final AbstractID datasetId;
    private boolean isCached;
    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     */
    public CacheTransformation(Transformation<T> transformationToCache, String name) {
        super(
                name,
                transformationToCache.getOutputType(),
                transformationToCache.getParallelism(),
                transformationToCache.isParallelismConfigured());
        this.transformationToCache = transformationToCache;

        this.datasetId = new AbstractID();
        this.isCached = false;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        if (isCached) {
            return result;
        }
        result.addAll(transformationToCache.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        if (isCached) {
            return Collections.emptyList();
        }
        return Collections.singletonList(transformationToCache);
    }

    public AbstractID getDatasetId() {
        return datasetId;
    }

    public Transformation<T> getTransformationToCache() {
        return transformationToCache;
    }

    public void setCached(boolean cached) {
        isCached = cached;
    }

    public boolean isCached() {
        return isCached;
    }
}
