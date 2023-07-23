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
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FileCatalogStore}. */
public class FileCatalogStoreTest {
    @Test
    void testFileCatalogStoreFactoryDiscovery(@TempDir File tempFolder) {

        String factoryIdentifier = FileCatalogStoreFactoryOptions.IDENTIFIER;
        Map<String, String> options = new HashMap<>();
        options.put("path", tempFolder.getAbsolutePath());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final FactoryUtil.DefaultCatalogStoreContext discoveryContext =
                new FactoryUtil.DefaultCatalogStoreContext(options, null, classLoader);
        final CatalogStoreFactory factory =
                FactoryUtil.discoverFactory(
                        classLoader, CatalogStoreFactory.class, factoryIdentifier);
        factory.open(discoveryContext);

        CatalogStore catalogStore = factory.createCatalogStore();
        assertThat(catalogStore instanceof FileCatalogStore).isTrue();

        factory.close();
    }

    @Test
    void testFileCatalogStoreSaveAndRead(@TempDir File tempFolder) throws IOException {
        CatalogStore catalogStore = new FileCatalogStore(tempFolder.getAbsolutePath(), "utf-8");
        catalogStore.open();

        Configuration catalogConfiguration = new Configuration();
        catalogConfiguration.setString("type", "generic_in_memory");

        // store catalog to catalog store
        catalogStore.storeCatalog("cat1", CatalogDescriptor.of("cat1", catalogConfiguration));
        catalogStore.storeCatalog("cat2", CatalogDescriptor.of("cat2", catalogConfiguration));

        assertTrue(catalogStore.contains("cat1"));
        assertTrue(catalogStore.contains("cat2"));

        // check catalog file is right created
        List<String> files =
                Arrays.stream(tempFolder.listFiles())
                        .map(file -> file.getName())
                        .collect(Collectors.toList());
        assertTrue(files.contains("cat1.yaml"));
        assertTrue(files.contains("cat2.yaml"));

        // check file content
        String yamlPath1 = String.format("%s/%s", tempFolder, "cat1.yaml");
        String content = FileUtils.readFileUtf8(new File(yamlPath1));
        assertThat(content).isEqualTo("type: generic_in_memory\n");

        catalogStore.close();

        // create a new FileCatalogStore, check catalog is right loaded.
        catalogStore = new FileCatalogStore(tempFolder.getAbsolutePath(), "utf-8");
        catalogStore.open();

        assertTrue(catalogStore.listCatalogs().contains("cat1"));
        assertTrue(catalogStore.listCatalogs().contains("cat2"));

        // test remove operation.
        catalogStore.removeCatalog("cat1", false);
        catalogStore.close();

        catalogStore = new FileCatalogStore(tempFolder.getAbsolutePath(), "utf-8");
        catalogStore.open();
        assertFalse(catalogStore.listCatalogs().contains("cat1"));
        assertTrue(catalogStore.listCatalogs().contains("cat2"));
    }

    @Test
    void testInvalidCases(@TempDir File tempFolder) {
        // test catalog store the catalog already exists.
        final CatalogStore catalogStore =
                new FileCatalogStore(tempFolder.getAbsolutePath(), "utf-8");
        catalogStore.open();

        Configuration catalogConfiguration = new Configuration();
        catalogConfiguration.setString("type", "generic_in_memory");

        // store catalog to catalog store
        catalogStore.storeCatalog("cat1", CatalogDescriptor.of("cat1", catalogConfiguration));
        assertThatThrownBy(
                        () ->
                                catalogStore.storeCatalog(
                                        "cat1", CatalogDescriptor.of("cat1", catalogConfiguration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Failed to save catalog cat1's configuration to file");

        // get a no exist catalog
        assertThat(catalogStore.getCatalog("cat3")).isEmpty();

        // remove a no exist catalog
        assertThatThrownBy(() -> catalogStore.removeCatalog("cat3", false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Failed to delete catalog cat3's store file");

        catalogStore.close();

        // test unsupported file schema
        CatalogStore catalogStore2 =
                new FileCatalogStore("hdfs://namenode:14565/some/path/to/a/file", "utf-8");
        catalogStore2.open();
        assertThatThrownBy(() -> catalogStore2.listCatalogs())
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("File catalog store only support local directory");
    }
}
