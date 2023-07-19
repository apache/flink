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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link GenericInMemoryCatalogStore}. */
public class GenericInMemoryCatalogStoreTest {

    @Test
    void testStoreAndGet() {
        CatalogStore catalogStore = new GenericInMemoryCatalogStore();
        catalogStore.open();

        catalogStore.storeCatalog(
                "catalog1", CatalogDescriptor.of("catalog1", new Configuration()));
        assertThat(catalogStore.getCatalog("catalog1").isPresent()).isTrue();
        assertThat(catalogStore.contains("catalog1")).isTrue();

        catalogStore.removeCatalog("catalog1", true);
        assertThat(catalogStore.contains("catalog1")).isFalse();

        assertThatThrownBy(() -> catalogStore.removeCatalog("catalog1", false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog catalog1 does not exist in the catalog store.");

        catalogStore.close();

        assertThatThrownBy(
                        () ->
                                catalogStore.storeCatalog(
                                        "catalog1",
                                        CatalogDescriptor.of("catalog1", new Configuration())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");

        assertThatThrownBy(() -> catalogStore.removeCatalog("catalog1", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");

        assertThatThrownBy(() -> catalogStore.contains("catalog1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");

        assertThatThrownBy(() -> catalogStore.listCatalogs())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");
    }
}
