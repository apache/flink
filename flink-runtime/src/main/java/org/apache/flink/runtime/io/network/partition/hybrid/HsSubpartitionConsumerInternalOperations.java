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

package org.apache.flink.runtime.io.network.partition.hybrid;

/**
 * Operations provided by {@link HsSubpartitionConsumer} that are used by other internal components
 * of hybrid result partition.
 */
public interface HsSubpartitionConsumerInternalOperations {

    /** Callback for new data become available. */
    void notifyDataAvailable();

    /**
     * Get the latest consuming offset of the subpartition.
     *
     * @param withLock If true, read the consuming offset outside the guarding of lock. This is
     *     sometimes desired to avoid lock contention, if the caller does not depend on any other
     *     states to change atomically with the consuming offset.
     * @return latest consuming offset.
     */
    int getConsumingOffset(boolean withLock);
}
