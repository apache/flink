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

package org.apache.flink.table.connector.source.lookup;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.functions.LookupFunction;

/**
 * A {@link LookupFunctionProvider} that never lookup in external system on cache miss and provides
 * a cache for holding all entries in the external system. The cache will be fully reloaded from the
 * external system by the {@link ScanTableSource.ScanRuntimeProvider} and reload operations will be
 * triggered by the {@link CacheReloadTrigger}.
 */
@PublicEvolving
public interface FullCachingLookupProvider extends LookupFunctionProvider {

    /**
     * Build a {@link FullCachingLookupProvider} from the specified {@link
     * ScanTableSource.ScanRuntimeProvider} and {@link CacheReloadTrigger}.
     */
    static FullCachingLookupProvider of(
            ScanTableSource.ScanRuntimeProvider scanRuntimeProvider,
            CacheReloadTrigger cacheReloadTrigger) {
        return new FullCachingLookupProvider() {
            @Override
            public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider() {
                return scanRuntimeProvider;
            }

            @Override
            public CacheReloadTrigger getCacheReloadTrigger() {
                return cacheReloadTrigger;
            }

            @Override
            public LookupFunction createLookupFunction() {
                return null;
            }
        };
    }

    /**
     * Get a {@link ScanTableSource.ScanRuntimeProvider} for scanning all entries from the external
     * lookup table and load into the cache.
     */
    ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider();

    /** Get a {@link CacheReloadTrigger} for triggering the reload operation. */
    CacheReloadTrigger getCacheReloadTrigger();
}
