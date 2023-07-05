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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogStore;

import java.util.Map;

/**
 * A factory to create configured catalog store instances based on string-based properties. See also
 * {@link Factory} for more information.
 */
@PublicEvolving
public interface CatalogStoreFactory extends Factory {

    /** Creates a {@link CatalogStore} instance from context information. */
    CatalogStore createCatalogStore(Context context);

    /** Initialization method for the CatalogStoreFactory. */
    void open(Context context);

    /** Tear-down method for the CatalogStoreFactory. */
    void close();

    /** Context provided when a catalog store is created. */
    @PublicEvolving
    interface Context {

        /**
         * Returns the options with which the catalog store is created.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getOptions();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();
    }

    /** Default implementation of {@link CatalogStoreFactory.Context}. */
    @Internal
    class DefaultCatalogStoreContext implements CatalogStoreFactory.Context {

        private Map<String, String> options;

        private ReadableConfig configuration;

        private ClassLoader classLoader;

        public static DefaultCatalogStoreContext of(
                Map<String, String> options,
                ReadableConfig configuration,
                ClassLoader classLoader) {
            return new DefaultCatalogStoreContext(options, configuration, classLoader);
        }

        private DefaultCatalogStoreContext(
                Map<String, String> options,
                ReadableConfig configuration,
                ClassLoader classLoader) {
            this.options = options;
            this.configuration = configuration;
            this.classLoader = classLoader;
        }

        @Override
        public Map<String, String> getOptions() {
            return options;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
