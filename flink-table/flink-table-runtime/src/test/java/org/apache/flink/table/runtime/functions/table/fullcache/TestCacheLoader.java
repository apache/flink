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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.CacheLoader;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;

/** Test realization of {@link CacheLoader}. */
public class TestCacheLoader extends CacheLoader {

    public static final Map<RowData, Collection<RowData>> DATA = new HashMap<>();

    static {
        DATA.put(row(1), Collections.singletonList(row(1, "Julian")));
        DATA.put(row(3), Arrays.asList(row(3, "Jark"), row(3, "Jackson")));
        DATA.put(row(4), Collections.singletonList(row(4, "Fabian")));
    }

    private final Consumer<Map<RowData, Collection<RowData>>> secondLoadDataChange;
    private int numLoads;
    private boolean isAwaitTriggered;

    public TestCacheLoader(Consumer<Map<RowData, Collection<RowData>>> secondLoadDataChange) {
        this.secondLoadDataChange = secondLoadDataChange;
    }

    public int getNumLoads() {
        return numLoads;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public boolean isAwaitTriggered() {
        return isAwaitTriggered;
    }

    @Override
    public void awaitFirstLoad() {
        isAwaitTriggered = true;
    }

    @Override
    protected boolean updateCache() {
        cache = new ConcurrentHashMap<>(DATA);
        numLoads++;
        if (numLoads == 2) {
            secondLoadDataChange.accept(cache);
        }
        return true;
    }
}
