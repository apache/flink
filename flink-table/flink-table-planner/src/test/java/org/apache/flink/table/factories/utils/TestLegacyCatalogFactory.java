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

package org.apache.flink.table.factories.utils;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Legacy catalog factory for testing. */
public class TestLegacyCatalogFactory implements CatalogFactory {

    public static final String CATALOG_TYPE_TEST_LEGACY = "test-legacy";

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CommonCatalogOptions.CATALOG_TYPE.key(), CATALOG_TYPE_TEST_LEGACY);
        context.put(FactoryUtil.PROPERTY_VERSION.key(), "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.emptyList();
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        return new GenericInMemoryCatalog(name);
    }
}
