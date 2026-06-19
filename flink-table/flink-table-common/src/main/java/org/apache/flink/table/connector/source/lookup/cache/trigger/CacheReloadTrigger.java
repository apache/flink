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

package org.apache.flink.table.connector.source.lookup.cache.trigger;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/** Customized trigger for reloading lookup table entries. */
@PublicEvolving
public interface CacheReloadTrigger extends AutoCloseable, Serializable {

    /** Open the trigger. */
    void open(Context context) throws Exception;

    /**
     * Context of {@link CacheReloadTrigger} for getting information about times and triggering
     * reload.
     */
    @PublicEvolving
    interface Context {

        /** Returns the current processing time. */
        long currentProcessingTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Trigger a reload operation on the cache. Runtime will wait for the first load to complete
         * to start an execution of lookup join.
         */
        CompletableFuture<Void> triggerReload();
    }
}
