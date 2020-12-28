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

package org.apache.flink.runtime.io.network.partition;

/** Encapsulates all information that a {@link PartitionTracker} keeps for a partition. */
public class PartitionTrackerEntry<K, M> {

    private final ResultPartitionID resultPartitionId;
    private final K key;
    private final M metaInfo;

    PartitionTrackerEntry(ResultPartitionID resultPartitionId, K key, M metaInfo) {
        this.resultPartitionId = resultPartitionId;
        this.key = key;
        this.metaInfo = metaInfo;
    }

    public ResultPartitionID getResultPartitionId() {
        return resultPartitionId;
    }

    public K getKey() {
        return key;
    }

    public M getMetaInfo() {
        return metaInfo;
    }
}
