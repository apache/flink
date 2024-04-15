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
import org.apache.flink.table.factories.FactoryUtil.DefaultCatalogStoreContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link GenericInMemoryCatalogStoreFactory}. */
public class GenericInMemoryCatalogStoreFactoryTest {

    @Test
    void testCatalogStoreInit() {
        String factoryIdentifier = GenericInMemoryCatalogStoreFactoryOptions.IDENTIFIER;
        Map<String, String> options = new HashMap<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final DefaultCatalogStoreContext discoveryContext =
                new DefaultCatalogStoreContext(options, null, classLoader);
        final CatalogStoreFactory factory =
                FactoryUtil.discoverFactory(
                        classLoader, CatalogStoreFactory.class, factoryIdentifier);
        factory.open(discoveryContext);

        CatalogStore catalogStore = factory.createCatalogStore();
        assertThat(catalogStore instanceof GenericInMemoryCatalogStore).isTrue();

        factory.close();
    }
}
