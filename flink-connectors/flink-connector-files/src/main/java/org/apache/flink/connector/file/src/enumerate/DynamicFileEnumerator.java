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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicFilteringData;

/**
 * {@code FileEnumerator} that supports dynamic filtering. The enumerator only enumerates splits
 * that exist in the given {@link DynamicFilteringData}, while enumerates all splits if no
 * DynamicFilteringData is provided when #enumerateSplits is called.
 */
@PublicEvolving
public interface DynamicFileEnumerator extends FileEnumerator {

    /**
     * Provides a {@link DynamicFilteringData} for filtering while the enumerator is enumerating
     * splits.
     *
     * <p>The {@link DynamicFilteringData} is typically collected by a collector operator, and
     * transferred here by a coordinating event. The method should never be called directly by
     * users.
     */
    void setDynamicFilteringData(DynamicFilteringData data);

    /** Factory for the {@link DynamicFileEnumerator}. */
    @FunctionalInterface
    interface Provider extends FileEnumerator.Provider {

        DynamicFileEnumerator create();
    }
}
