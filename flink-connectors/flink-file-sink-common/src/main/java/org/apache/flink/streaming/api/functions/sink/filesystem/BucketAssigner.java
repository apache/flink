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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A BucketAssigner is used with a file sink to determine the bucket each incoming element should be
 * put into.
 *
 * <p>The {@code StreamingFileSink} can be writing to many buckets at a time, and it is responsible
 * for managing a set of active buckets. Whenever a new element arrives it will ask the {@code
 * BucketAssigner} for the bucket the element should fall in. The {@code BucketAssigner} can, for
 * example, determine buckets based on system time.
 *
 * @param <IN> The type of input elements.
 * @param <BucketID> The type of the object returned by the {@link #getBucketId(Object,
 *     BucketAssigner.Context)}. This has to have a correct {@link #hashCode()} and {@link
 *     #equals(Object)} method. In addition, the {@link Path} to the created bucket will be the
 *     result of the {@link #toString()} of this method, appended to the {@code basePath} specified
 *     in the file sink.
 */
@PublicEvolving
public interface BucketAssigner<IN, BucketID> extends Serializable {

    /**
     * Returns the identifier of the bucket the provided element should be put into.
     *
     * @param element The current element being processed.
     * @param context The context used by the current bucket assigner.
     * @return A string representing the identifier of the bucket the element should be put into.
     *     The actual path to the bucket will result from the concatenation of the returned string
     *     and the {@code base path} provided during the initialization of the file sink.
     */
    BucketID getBucketId(IN element, BucketAssigner.Context context);

    /**
     * @return A {@link SimpleVersionedSerializer} capable of serializing/deserializing the elements
     *     of type {@code BucketID}. That is the type of the objects returned by the {@link
     *     #getBucketId(Object, BucketAssigner.Context)}.
     */
    SimpleVersionedSerializer<BucketID> getSerializer();

    /**
     * Context that the {@link BucketAssigner} can use for getting additional data about an input
     * record.
     *
     * <p>The context is only valid for the duration of a {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} call. Do not store the context and use afterwards!
     */
    @PublicEvolving
    interface Context {

        /** Returns the current processing time. */
        long currentProcessingTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or {@code null} if the element does not
         * have an assigned timestamp.
         */
        @Nullable
        Long timestamp();
    }
}
