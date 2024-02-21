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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/** Utils for the executable operation. */
@Internal
public class ExecutableOperationUtils {

    /**
     * Creates a {@link DynamicTableSink} from a {@link CatalogTable}.
     *
     * <p>It'll try to create table sink from to {@param catalog}, then try to create from {@param
     * sinkFactorySupplier} passed secondly. Otherwise, an attempt is made to discover a matching
     * factory using Java SPI (see {@link Factory} for details).
     */
    public static DynamicTableSink createDynamicTableSink(
            @Nullable Catalog catalog,
            Supplier<Optional<DynamicTableSinkFactory>> sinkFactorySupplier,
            ObjectIdentifier objectIdentifier,
            ResolvedCatalogTable catalogTable,
            Map<String, String> enrichmentOptions,
            ReadableConfig configuration,
            ClassLoader classLoader,
            boolean isTemporary) {
        DynamicTableSinkFactory dynamicTableSinkFactory = null;
        if (catalog != null
                && catalog.getFactory().isPresent()
                && catalog.getFactory().get() instanceof DynamicTableSinkFactory) {
            // try get from catalog
            dynamicTableSinkFactory = (DynamicTableSinkFactory) catalog.getFactory().get();
        }
        if (dynamicTableSinkFactory == null) {
            dynamicTableSinkFactory = sinkFactorySupplier.get().orElse(null);
        }
        return FactoryUtil.createDynamicTableSink(
                dynamicTableSinkFactory,
                objectIdentifier,
                catalogTable,
                enrichmentOptions,
                configuration,
                classLoader,
                isTemporary);
    }
}
