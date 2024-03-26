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

import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

/**
 * Tests for {@link CatalogManager} and {@link Catalog} which requires proper thread context
 * classloader.
 */
public class CatalogManagerContextClassloaderTest {
    @Test
    public void testOpenCatalogWithContextClassloader() throws MalformedURLException {
        final String catalogJarFile = "target/test-catalog-jar-test-jar.jar";
        CatalogStoreHolder catalogStoreHolder =
                CatalogStoreHolder.newBuilder()
                        .catalogStore(new GenericInMemoryCatalogStore())
                        .config(new Configuration())
                        .classloader(
                                new URLClassLoader(
                                        new URL[] {new File(catalogJarFile).toURI().toURL()},
                                        Thread.currentThread().getContextClassLoader()))
                        .build();
        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(CatalogManagerTest.class.getClassLoader())
                        .config(new Configuration())
                        .defaultCatalog("default", new GenericInMemoryCatalog("default"))
                        .catalogStoreHolder(catalogStoreHolder)
                        .build();
        catalogManager.createCatalog(
                "test",
                CatalogDescriptor.of(
                        "test", Configuration.fromMap(Collections.singletonMap("type", "test"))));
    }
}
