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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend.PriorityQueueStateType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.TIMER_SERVICE_FACTORY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The configuration of rocksDB priority queue state implementation. */
public class RocksDBPriorityQueueConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int UNDEFINED_ROCKSDB_PRIORITY_QUEUE_SET_CACHE_SIZE = -1;

    /** This determines the type of priority queue state. */
    private @Nullable PriorityQueueStateType priorityQueueStateType;

    /** cache size per keyGroup for rocksDB priority queue state. */
    private int rocksDBPriorityQueueSetCacheSize;

    public RocksDBPriorityQueueConfig() {
        this(null, UNDEFINED_ROCKSDB_PRIORITY_QUEUE_SET_CACHE_SIZE);
    }

    public RocksDBPriorityQueueConfig(
            PriorityQueueStateType priorityQueueStateType, int rocksDBPriorityQueueSetCacheSize) {
        this.priorityQueueStateType = priorityQueueStateType;
        this.rocksDBPriorityQueueSetCacheSize = rocksDBPriorityQueueSetCacheSize;
    }

    /**
     * Gets the type of the priority queue state. It will fall back to the default value if it is
     * not explicitly set.
     */
    public PriorityQueueStateType getPriorityQueueStateType() {
        return priorityQueueStateType == null
                ? TIMER_SERVICE_FACTORY.defaultValue()
                : priorityQueueStateType;
    }

    public void setPriorityQueueStateType(PriorityQueueStateType type) {
        this.priorityQueueStateType = checkNotNull(type);
    }

    /**
     * Gets the cache size of rocksDB priority queue set. It will fall back to the default value if
     * it is not explicitly set.
     */
    public int getRocksDBPriorityQueueSetCacheSize() {
        return rocksDBPriorityQueueSetCacheSize == UNDEFINED_ROCKSDB_PRIORITY_QUEUE_SET_CACHE_SIZE
                ? ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE.defaultValue()
                : rocksDBPriorityQueueSetCacheSize;
    }

    public static RocksDBPriorityQueueConfig fromOtherAndConfiguration(
            RocksDBPriorityQueueConfig other, ReadableConfig config) {
        PriorityQueueStateType priorityQueueType =
                (null == other.priorityQueueStateType)
                        ? config.get(TIMER_SERVICE_FACTORY)
                        : other.priorityQueueStateType;
        int cacheSize =
                (other.rocksDBPriorityQueueSetCacheSize
                                == UNDEFINED_ROCKSDB_PRIORITY_QUEUE_SET_CACHE_SIZE)
                        ? config.get(ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE)
                        : other.rocksDBPriorityQueueSetCacheSize;
        return new RocksDBPriorityQueueConfig(priorityQueueType, cacheSize);
    }

    public static RocksDBPriorityQueueConfig buildWithPriorityQueueType(
            PriorityQueueStateType type) {
        return new RocksDBPriorityQueueConfig(
                type, ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE.defaultValue());
    }
}
