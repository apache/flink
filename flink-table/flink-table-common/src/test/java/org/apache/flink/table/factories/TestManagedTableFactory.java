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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/** A test {@link ManagedTableFactory}. */
public class TestManagedTableFactory implements DynamicTableSourceFactory, ManagedTableFactory {

    public static final String ENRICHED_KEY = "ENRICHED_KEY";

    public static final String ENRICHED_VALUE = "ENRICHED_VALUE";

    public static final Map<ObjectIdentifier, AtomicReference<Map<String, String>>> MANAGED_TABLES =
            new ConcurrentHashMap<>();

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public Map<String, String> enrichOptions(Context context) {
        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        if (MANAGED_TABLES.containsKey(context.getObjectIdentifier())) {
            newOptions.put(ENRICHED_KEY, ENRICHED_VALUE);
        }
        return newOptions;
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        MANAGED_TABLES.compute(
                context.getObjectIdentifier(),
                (k, v) -> {
                    if (v != null) {
                        if (v.get() == null) {
                            v.set(context.getCatalogTable().getOptions());
                        } else if (!ignoreIfExists) {
                            throw new TableException("Table exists.");
                        }
                    }
                    return v;
                });
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        AtomicReference<Map<String, String>> reference =
                MANAGED_TABLES.get(context.getObjectIdentifier());
        if (reference != null) {
            Map<String, String> previous = reference.getAndSet(null);
            if (!context.getCatalogTable().getOptions().equals(previous) && !ignoreIfNotExists) {
                throw new TableException("Table does not exist.");
            }
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return new TestManagedTableSource();
    }

    /** Managed {@link DynamicTableSource} for testing. */
    public static class TestManagedTableSource implements ScanTableSource {

        @Override
        public ChangelogMode getChangelogMode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSource copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String asSummaryString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }
    }
}
