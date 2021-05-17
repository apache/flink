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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * Base interface for configuring a dynamic table connector for an external storage system from
 * catalog and session information.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion.
 *
 * <p>Implement {@link DynamicTableSourceFactory} for constructing a {@link DynamicTableSource}.
 *
 * <p>Implement {@link DynamicTableSinkFactory} for constructing a {@link DynamicTableSink}.
 *
 * <p>The options {@link FactoryUtil#PROPERTY_VERSION} and {@link FactoryUtil#CONNECTOR} are
 * implicitly added and must not be declared.
 */
@PublicEvolving
public interface DynamicTableFactory extends Factory {

    /** Provides catalog and session information describing the dynamic table to be accessed. */
    interface Context {

        /** Returns the identifier of the table in the {@link Catalog}. */
        ObjectIdentifier getObjectIdentifier();

        /**
         * Returns the resolved table information received from the {@link Catalog}.
         *
         * <p>The {@link ResolvedCatalogTable} forwards the metadata from the catalog but offers a
         * validated {@link ResolvedSchema}. The original metadata object is available via {@link
         * ResolvedCatalogTable#getOrigin()}.
         *
         * <p>In most cases, a factory is interested in the following two characteristics:
         *
         * <pre>{@code
         * // get the physical data type to initialize the connector
         * context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
         *
         * // get primary key information if the connector supports upserts
         * context.getCatalogTable().getResolvedSchema().getPrimaryKey()
         * }</pre>
         *
         * <p>Other characteristics such as metadata columns or watermarks will be pushed down into
         * the created {@link DynamicTableSource} or {@link DynamicTableSink} during planning
         * depending on the implemented ability interfaces.
         */
        ResolvedCatalogTable getCatalogTable();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();

        /** Whether the table is temporary. */
        boolean isTemporary();
    }
}
