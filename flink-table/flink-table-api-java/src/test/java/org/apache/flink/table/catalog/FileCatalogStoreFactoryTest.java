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

import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link FileCatalogStoreFactory}. */
class FileCatalogStoreFactoryTest {

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
}
