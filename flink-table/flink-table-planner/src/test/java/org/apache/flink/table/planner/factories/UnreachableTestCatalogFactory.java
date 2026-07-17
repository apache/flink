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

package org.apache.flink.table.planner.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;

/**
 * Test catalog factory that creates a catalog whose {@link Catalog#databaseExists(String)} always
 * fails, simulating a catalog that cannot be reached (e.g. a connectivity failure).
 */
public class UnreachableTestCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "test-unreachable-catalog";

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
        return Collections.emptySet();
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.createCatalogFactoryHelper(this, context).validate();
        return new UnreachableCatalog(context.getName());
    }

    /** A catalog whose {@link #databaseExists(String)} always fails. */
    public static class UnreachableCatalog extends GenericInMemoryCatalog {
        public UnreachableCatalog(String name) {
            super(name, "default");
        }

        @Override
        public boolean databaseExists(String databaseName) {
            throw new CatalogException(
                    "Failed to connect to Kafka cluster for database '"
                            + databaseName
                            + "' of catalog '"
                            + getName()
                            + "'.");
        }
    }
}
