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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HADOOP_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_VERSION;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_TYPE_VALUE_HIVE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/** Catalog factory for {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogFactory.class);

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_HIVE); // hive
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // default database
        properties.add(CATALOG_DEFAULT_DATABASE);

        properties.add(CATALOG_HIVE_CONF_DIR);

        properties.add(CATALOG_HIVE_VERSION);

        properties.add(CATALOG_HADOOP_CONF_DIR);

        return properties;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        final String defaultDatabase =
                descriptorProperties
                        .getOptionalString(CATALOG_DEFAULT_DATABASE)
                        .orElse(HiveCatalog.DEFAULT_DB);

        final Optional<String> hiveConfDir =
                descriptorProperties.getOptionalString(CATALOG_HIVE_CONF_DIR);

        final Optional<String> hadoopConfDir =
                descriptorProperties.getOptionalString(CATALOG_HADOOP_CONF_DIR);

        final String version =
                descriptorProperties
                        .getOptionalString(CATALOG_HIVE_VERSION)
                        .orElse(HiveShimLoader.getHiveVersion());

        return new HiveCatalog(
                name,
                defaultDatabase,
                hiveConfDir.orElse(null),
                hadoopConfDir.orElse(null),
                version);
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new HiveCatalogValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
