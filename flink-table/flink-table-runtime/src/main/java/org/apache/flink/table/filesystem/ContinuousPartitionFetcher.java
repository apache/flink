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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.List;

/**
 * Partition fetcher for helping continuously fetch partitioned table.
 *
 * @param <P> The type of partition.
 * @param <T> The type of partition offset, the type could be Long when fetches in partition-time or
 *     create-time order, be String when fetches in partition-name order.
 */
@Internal
public interface ContinuousPartitionFetcher<P, T extends Comparable<T>>
        extends PartitionFetcher<P> {

    /** Fetch partitions by previous partition offset (Including). */
    List<Tuple2<P, T>> fetchPartitions(Context<P, T> context, T previousOffset) throws Exception;

    /**
     * Context for fetch partitions, partition information is stored in hive meta store.
     *
     * @param <P> The type of partition.
     * @param <T> The type of partition offset, the type could be Long when fetches in
     *     partition-time or create-time order, be String when fetches in partition-name order.
     */
    interface Context<P, T extends Comparable<T>> extends PartitionFetcher.Context<P> {
        /** The table full path. */
        ObjectPath getTablePath();

        /** Get the Serializer of partition order. */
        TypeSerializer<T> getTypeSerializer();

        /** Get the partition consume start offset. */
        T getConsumeStartOffset();
    }
}
