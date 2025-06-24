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

package org.apache.flink.table.catalog.listener;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.Factory;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Context for catalog which provides the name, factory identifier and configuration to identify the
 * same physical catalog for different logical catalog. For example, catalogs with different catalog
 * name may be created on the same physical catalog.
 */
@PublicEvolving
public interface CatalogContext {
    /** The catalog name. */
    String getCatalogName();

    /**
     * Identifier for the catalog from factory which is used to create dynamic tables. Notice that
     * the factory for hive catalog will throw an exception, you can use getClazz to get catalog
     * class.
     */
    Optional<String> getFactoryIdentifier();

    /** Class of the catalog. */
    @Nullable
    Class<? extends Catalog> getClazz();

    /** The catalog configuration. */
    Configuration getConfiguration();

    static CatalogContext createContext(final String catalogName, final Catalog catalog) {
        return new CatalogContext() {

            @Override
            public String getCatalogName() {
                return catalogName;
            }

            @Override
            public Optional<String> getFactoryIdentifier() {
                return catalog == null
                        ? Optional.empty()
                        : catalog.getFactory().map(Factory::factoryIdentifier);
            }

            @Override
            @Nullable
            public Class<? extends Catalog> getClazz() {
                return catalog == null ? null : catalog.getClass();
            }

            /**
             * TODO After https://issues.apache.org/jira/browse/FLINK-32427 is finished, we can get
             * configuration for catalog.
             */
            @Override
            public Configuration getConfiguration() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
