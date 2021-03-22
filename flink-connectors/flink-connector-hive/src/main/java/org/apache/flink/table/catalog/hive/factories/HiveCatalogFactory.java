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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HADOOP_CONF_DIR;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HIVE_VERSION;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Catalog factory for {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return HiveCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(PROPERTY_VERSION);
        options.add(HIVE_CONF_DIR);
        options.add(HIVE_VERSION);
        options.add(HADOOP_CONF_DIR);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new HiveCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(HIVE_CONF_DIR),
                helper.getOptions().get(HADOOP_CONF_DIR),
                helper.getOptions().get(HIVE_VERSION));
    }
}
