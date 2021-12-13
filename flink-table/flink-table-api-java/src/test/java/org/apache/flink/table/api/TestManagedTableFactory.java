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

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.ManagedTableFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** A test {@link ManagedTableFactory}. */
public class TestManagedTableFactory implements ManagedTableFactory {

    public static final String ENRICHED_KEY = "ENRICHED_KEY";

    public static final String ENRICHED_VALUE = "ENRICHED_VALUE";

    public static final Map<ObjectIdentifier, Map<String, String>> MANAGED_TABLES =
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
        newOptions.put(ENRICHED_KEY, ENRICHED_VALUE);
        return newOptions;
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        MANAGED_TABLES.compute(
                context.getObjectIdentifier(),
                (k, v) -> {
                    if (v == null) {
                        return context.getCatalogTable().toProperties();
                    } else if (!ignoreIfExists) {
                        throw new TableException("Table exists.");
                    } else {
                        return v;
                    }
                });
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        boolean remove =
                MANAGED_TABLES.remove(
                        context.getObjectIdentifier(), context.getCatalogTable().toProperties());
        if (!remove && !ignoreIfNotExists) {
            throw new TableException("Table not exists.");
        }
    }
}
