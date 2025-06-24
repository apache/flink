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

package org.apache.flink.table.runtime.functions.table.fullcache;

import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;

/** Test implementation of {@link CacheReloadTrigger} that is triggered manually. */
public class TestManualCacheReloadTrigger implements CacheReloadTrigger {

    private Context context;
    private boolean isClosed;

    public void trigger() throws Exception {
        if (context != null) {
            context.triggerReload().get();
        }
    }

    @Override
    public void open(Context context) throws Exception {
        this.context = context;
        trigger();
    }

    @Override
    public void close() throws Exception {
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }
}
