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

package org.apache.flink.table.api.internal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TablePipeline;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableWritePrivilege;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableImpl}. */
class TableImplTest {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder().column("id", DataTypes.INT()).build();

    private TableEnvironmentMock tEnv;
    private CompletableFuture<Set<TableWritePrivilege>> capturedPrivileges;

    @BeforeEach
    void setUp() {
        capturedPrivileges = new CompletableFuture<>();

        // Create a custom catalog that captures privileges
        GenericInMemoryCatalog testCatalog =
                new GenericInMemoryCatalog(
                        CatalogManagerMocks.DEFAULT_CATALOG, CatalogManagerMocks.DEFAULT_DATABASE) {
                    @Override
                    public CatalogBaseTable getTable(
                            ObjectPath tablePath, Set<TableWritePrivilege> privileges)
                            throws TableNotExistException {
                        capturedPrivileges.complete(privileges);
                        return super.getTable(tablePath);
                    }
                };

        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(TableImplTest.class.getClassLoader())
                        .config(new Configuration())
                        .defaultCatalog(CatalogManagerMocks.DEFAULT_CATALOG, testCatalog)
                        .catalogStoreHolder(
                                CatalogStoreHolder.newBuilder()
                                        .catalogStore(new GenericInMemoryCatalogStore())
                                        .config(new Configuration())
                                        .classloader(TableImplTest.class.getClassLoader())
                                        .build())
                        .build();

        tEnv = TableEnvironmentMock.getStreamingInstance(catalogManager);
    }

    @Test
    void testInsertIntoPassesInsertPrivilege() throws Exception {
        // Create a source and sink table
        tEnv.createTable(
                "source_table", TableDescriptor.forConnector("fake").schema(TEST_SCHEMA).build());
        tEnv.createTable(
                "sink_table", TableDescriptor.forConnector("fake").schema(TEST_SCHEMA).build());

        // Get a Table from source
        Table sourceTable = tEnv.from("source_table");

        // Call insertInto with table path
        TablePipeline pipeline = sourceTable.insertInto("sink_table");

        // Verify that INSERT privilege was passed
        Set<TableWritePrivilege> privileges = capturedPrivileges.get(10, TimeUnit.SECONDS);
        assertThat(privileges).containsExactly(TableWritePrivilege.INSERT);

        // Verify the returned pipeline
        assertThat(pipeline).isNotNull();
        assertThat(pipeline).isInstanceOf(TablePipelineImpl.class);
        SinkModifyOperation sinkOp =
                (SinkModifyOperation) ((TablePipelineImpl) pipeline).getOperation();
        assertThat(sinkOp.getContextResolvedTable().getIdentifier().getObjectName())
                .isEqualTo("sink_table");
    }
}
