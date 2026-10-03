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

package org.apache.flink.state.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating {@link StateCatalog} instances via SQL DDL or programmatically.
 *
 * <p>Directories are configured with {@code directory.{label}} options:
 *
 * <pre>{@code
 * CREATE CATALOG state WITH (
 *     'type'              = 'state',
 *     'directory.my-app'  = '/checkpoints/app1',
 *     'directory.staging' = '/savepoints/staging'
 * );
 * }</pre>
 */
@PublicEvolving
public class StateCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "state";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                StateCatalogOptions.LISTING_PARALLELISM, StateCatalogOptions.DB_NAME_INCLUDE_TS);
    }

    @Override
    public Catalog createCatalog(Context context) {
        Map<String, String> options = context.getOptions();

        Map<String, String> labelsToDirs = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith(StateCatalogOptions.DIRECTORY_PREFIX)) {
                String label =
                        entry.getKey().substring(StateCatalogOptions.DIRECTORY_PREFIX.length());
                labelsToDirs.put(label, entry.getValue());
            }
        }

        Configuration configuration = Configuration.fromMap(options);
        return new StateCatalog(
                context.getName(),
                labelsToDirs,
                configuration.get(StateCatalogOptions.LISTING_PARALLELISM),
                configuration.get(StateCatalogOptions.DB_NAME_INCLUDE_TS));
    }
}
