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

import org.apache.flink.table.api.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Base of tests for any catalog implementations, like GenericInMemoryCatalog and HiveCatalog. */
public abstract class CatalogTestBase extends CatalogTest {

    @Override
    public CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                        putAll(getGenericFlag(isGeneric()));
                    }
                },
                TEST_COMMENT);
    }

    @Override
    public CatalogDatabase createAnotherDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k2", "v2");
                        putAll(getGenericFlag(isGeneric()));
                    }
                },
                TEST_COMMENT);
    }

    @Override
    public CatalogTable createTable() {
        final ResolvedSchema resolvedSchema = createSchema();
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        Collections.emptyList(),
                        getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogTable createAnotherTable() {
        final ResolvedSchema resolvedSchema = createAnotherSchema();
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        Collections.emptyList(),
                        getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogTable createStreamingTable() {
        final ResolvedSchema resolvedSchema = createSchema();
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        Collections.emptyList(),
                        getStreamingTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogTable createPartitionedTable() {
        final ResolvedSchema resolvedSchema = createSchema();
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        createPartitionKeys(),
                        getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogTable createAnotherPartitionedTable() {
        final ResolvedSchema resolvedSchema = createAnotherSchema();
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        createPartitionKeys(),
                        getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogPartition createPartition() {
        return new CatalogPartitionImpl(getBatchTableProperties(), TEST_COMMENT);
    }

    @Override
    public CatalogView createView() {
        final ResolvedSchema resolvedSchema = createSchema();
        final CatalogView origin =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "This is a view",
                        String.format("select * from %s", t1),
                        String.format(
                                "select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
                        getBatchTableProperties());
        return new ResolvedCatalogView(origin, resolvedSchema);
    }

    @Override
    public CatalogView createAnotherView() {
        final ResolvedSchema resolvedSchema = createAnotherSchema();
        final CatalogView origin =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "This is a another view",
                        String.format("select * from %s", t2),
                        String.format(
                                "select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
                        getBatchTableProperties());
        return new ResolvedCatalogView(origin, resolvedSchema);
    }

    protected Map<String, String> getBatchTableProperties() {
        return new HashMap<String, String>() {
            {
                put(IS_STREAMING, "false");
                putAll(getGenericFlag(isGeneric()));
            }
        };
    }

    protected Map<String, String> getStreamingTableProperties() {
        return new HashMap<String, String>() {
            {
                put(IS_STREAMING, "true");
                putAll(getGenericFlag(isGeneric()));
            }
        };
    }

    private Map<String, String> getGenericFlag(boolean isGeneric) {
        return new HashMap<String, String>() {
            {
                put(CatalogPropertiesUtil.IS_GENERIC, String.valueOf(isGeneric));
            }
        };
    }

    /** Whether the test meta-object is generic or not. */
    protected abstract boolean isGeneric();
}
