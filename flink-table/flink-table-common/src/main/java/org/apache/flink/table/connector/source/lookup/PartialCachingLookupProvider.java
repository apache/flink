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
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.functions.LookupFunction;

/**
 * Provider for creating {@link LookupFunction} and {@link LookupCache} for storing lookup entries.
 */
@PublicEvolving
public interface PartialCachingLookupProvider extends LookupFunctionProvider {

    /**
     * Build a {@link PartialCachingLookupProvider} from the specified {@link LookupFunction} and
     * {@link LookupCache}.
     */
    static PartialCachingLookupProvider of(LookupFunction lookupFunction, LookupCache cache) {
        return new PartialCachingLookupProvider() {

            @Override
            public LookupCache getCache() {
                return cache;
            }

            @Override
            public LookupFunction createLookupFunction() {
                return lookupFunction;
            }
        };
    }

    /** Get a new instance of {@link LookupCache}. */
    LookupCache getCache();
}
