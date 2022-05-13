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

package org.apache.flink.contrib.streaming.state;

/** Bridge utility between the old and new enum type. */
@SuppressWarnings("deprecation")
class LegacyEnumBridge {

    private LegacyEnumBridge() {}

    static RocksDBStateBackend.PriorityQueueStateType convert(
            EmbeddedRocksDBStateBackend.PriorityQueueStateType t) {
        switch (t) {
            case HEAP:
                return RocksDBStateBackend.PriorityQueueStateType.HEAP;
            case ROCKSDB:
                return RocksDBStateBackend.PriorityQueueStateType.ROCKSDB;
            default:
                throw new IllegalStateException("Unknown enum type " + t);
        }
    }

    static EmbeddedRocksDBStateBackend.PriorityQueueStateType convert(
            RocksDBStateBackend.PriorityQueueStateType t) {
        switch (t) {
            case HEAP:
                return EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP;
            case ROCKSDB:
                return EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB;
            default:
                throw new IllegalStateException("Unknown enum type " + t);
        }
    }
}
