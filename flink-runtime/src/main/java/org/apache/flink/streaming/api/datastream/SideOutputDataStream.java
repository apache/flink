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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;

/**
 * A {@link SideOutputDataStream} represents a {@link DataStream} that contains elements that are
 * emitted from upstream into a side output with some tag.
 *
 * @param <T> The type of the elements in this stream.
 */
@Public
public class SideOutputDataStream<T> extends DataStream<T> {
    /**
     * Creates a new {@link SideOutputDataStream} in the given execution environment.
     *
     * @param environment The StreamExecutionEnvironment
     * @param transformation The SideOutputTransformation
     */
    public SideOutputDataStream(
            StreamExecutionEnvironment environment, SideOutputTransformation<T> transformation) {
        super(environment, transformation);
    }

    /**
     * Caches the intermediate result of the transformation. Only support bounded streams and
     * currently only block mode is supported. The cache is generated lazily at the first time the
     * intermediate result is computed. The cache will be clear when {@link
     * CachedDataStream#invalidate()} called or the {@link StreamExecutionEnvironment} close.
     *
     * @return CachedDataStream that can use in later job to reuse the cached intermediate result.
     */
    @PublicEvolving
    public CachedDataStream<T> cache() {
        return new CachedDataStream<>(this.environment, this.transformation);
    }
}
