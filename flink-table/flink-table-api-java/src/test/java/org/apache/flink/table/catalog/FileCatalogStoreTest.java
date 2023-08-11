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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link FileCatalogStore}. */
class FileCatalogStoreTest {

    private static final String CATALOG_STORE_DIR_NAME = "dummy-catalog-store";
    private static final String DUMMY = "dummy";
    private static final CatalogDescriptor DUMMY_CATALOG;

    static {
        Configuration conf = new Configuration();
        conf.set(CommonCatalogOptions.CATALOG_TYPE, DUMMY);
        conf.set(GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE, "dummy_db");

        DUMMY_CATALOG = CatalogDescriptor.of(DUMMY, conf);
    }

    @TempDir private Path tempDir;

    @Test
    void testNotOpened() {
        CatalogStore catalogStore = initCatalogStore(false);

        assertCatalogStoreNotOpened(catalogStore::listCatalogs);
        assertCatalogStoreNotOpened(() -> catalogStore.contains(DUMMY));
        assertCatalogStoreNotOpened(() -> catalogStore.getCatalog(DUMMY));
        assertCatalogStoreNotOpened(() -> catalogStore.storeCatalog(DUMMY, DUMMY_CATALOG));
        assertCatalogStoreNotOpened(() -> catalogStore.removeCatalog(DUMMY, true));
    }

    @Test
    void testStoreDirNotExists() {
        CatalogStore catalogStore = initCatalogStore(false);
        Path catalogStorePath = tempDir.resolve(CATALOG_STORE_DIR_NAME);

        assertThatThrownBy(catalogStore::open)
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(
                        "Failed to open catalog store. The catalog store directory "
                                + catalogStorePath
                                + " does not exist.");
    }

    @Test
    void testStore() {
        CatalogStore catalogStore = initCatalogStore(true);
        catalogStore.open();

        catalogStore.storeCatalog(DUMMY, DUMMY_CATALOG);

        File catalog = getCatalogFile();
        assertThat(catalog.exists()).isTrue();
        assertThat(catalog.isFile()).isTrue();
        assertThat(catalogStore.contains(DUMMY)).isTrue();

        Set<String> storedCatalogs = catalogStore.listCatalogs();
        assertThat(storedCatalogs.size()).isEqualTo(1);
        assertThat(storedCatalogs.contains(DUMMY)).isTrue();
    }

    @Test
    void testRemoveExisting() {
        CatalogStore catalogStore = initCatalogStore(true);
        catalogStore.open();

        catalogStore.storeCatalog(DUMMY, DUMMY_CATALOG);
        assertThat(catalogStore.listCatalogs().size()).isEqualTo(1);

        catalogStore.removeCatalog(DUMMY, false);
        assertThat(catalogStore.listCatalogs().size()).isEqualTo(0);
        assertThat(catalogStore.contains(DUMMY)).isFalse();

        File catalog = getCatalogFile();
        assertThat(catalog.exists()).isFalse();
    }

    @Test
    void testRemoveNonExisting() {
        CatalogStore catalogStore = initCatalogStore(true);
        catalogStore.open();

        catalogStore.removeCatalog(DUMMY, true);

        File catalog = getCatalogFile();
        assertThatThrownBy(() -> catalogStore.removeCatalog(DUMMY, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(
                        "Catalog " + DUMMY + "'s store file " + catalog + " does not exist.");
    }

    @Test
    void testClose() {
        CatalogStore catalogStore = initCatalogStore(true);
        catalogStore.open();

        catalogStore.storeCatalog(DUMMY, DUMMY_CATALOG);
        assertThat(catalogStore.listCatalogs().size()).isEqualTo(1);

        catalogStore.close();

        assertCatalogStoreNotOpened(catalogStore::listCatalogs);
        assertCatalogStoreNotOpened(() -> catalogStore.contains(DUMMY));
        assertCatalogStoreNotOpened(() -> catalogStore.getCatalog(DUMMY));
        assertCatalogStoreNotOpened(() -> catalogStore.storeCatalog(DUMMY, DUMMY_CATALOG));
        assertCatalogStoreNotOpened(() -> catalogStore.removeCatalog(DUMMY, true));
    }

    private void assertCatalogStoreNotOpened(
            ThrowableAssert.ThrowingCallable shouldRaiseThrowable) {
        assertThatThrownBy(shouldRaiseThrowable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");
    }

    private CatalogStore initCatalogStore(boolean createDir) {
        Path catalogStorePath = tempDir.resolve(CATALOG_STORE_DIR_NAME);
        if (createDir) {
            catalogStorePath.toFile().mkdir();
        }

        return new FileCatalogStore(catalogStorePath.toString());
    }

    private File getCatalogFile() {
        return tempDir.resolve(CATALOG_STORE_DIR_NAME)
                .resolve(DUMMY + FileCatalogStore.FILE_EXTENSION)
                .toFile();
    }
}
