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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil.FactoryHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.FileCatalogStoreFactoryOptions.IDENTIFIER;
import static org.apache.flink.table.catalog.FileCatalogStoreFactoryOptions.PATH;
import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;

/** Catalog store factory for {@link FileCatalogStore}. */
@Internal
public class FileCatalogStoreFactory implements CatalogStoreFactory {

    private String path;

    @Override
    public CatalogStore createCatalogStore() {
        return new FileCatalogStore(path);
    }

    @Override
    public void open(Context context) {
        FactoryHelper<CatalogStoreFactory> factoryHelper =
                createCatalogStoreFactoryHelper(this, context);
        factoryHelper.validate();

        ReadableConfig options = factoryHelper.getOptions();
        path = options.get(PATH);
    }

    @Override
    public void close() {}

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);

        return Collections.unmodifiableSet(options);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
