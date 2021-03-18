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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Catalog factory for {@link GenericInMemoryCatalog}. */
public class GenericInMemoryCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return GenericInMemoryCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(PROPERTY_VERSION);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final Configuration configuration = Configuration.fromMap(context.getOptions());
        final String defaultDatabase = configuration.getString(DEFAULT_DATABASE);
        if (defaultDatabase == null || defaultDatabase.isEmpty()) {
            throw new ValidationException("The default database must not be empty");
        }

        return new GenericInMemoryCatalog(context.getName(), defaultDatabase);
    }
}
