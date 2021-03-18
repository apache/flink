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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.CatalogFactory;

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
        final Configuration configuration = Configuration.fromMap(context.getOptions());
        validateConfiguration(configuration);

        return new HiveCatalog(
                context.getName(),
                configuration.getString(DEFAULT_DATABASE),
                configuration.getString(HIVE_CONF_DIR),
                configuration.getString(HADOOP_CONF_DIR),
                configuration.getString(HIVE_VERSION));
    }

    private void validateConfiguration(Configuration configuration) {
        final String defaultDatabase = configuration.getString(DEFAULT_DATABASE);
        if (defaultDatabase != null && defaultDatabase.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' was provided, but is empty", DEFAULT_DATABASE.key()));
        }

        final String hiveConfDir = configuration.getString(HIVE_CONF_DIR);
        if (hiveConfDir != null && hiveConfDir.isEmpty()) {
            throw new ValidationException(
                    String.format("Option '%s' was provided, but is empty", HIVE_CONF_DIR.key()));
        }

        final String hadoopConfDir = configuration.getString(HADOOP_CONF_DIR);
        if (hadoopConfDir != null && hadoopConfDir.isEmpty()) {
            throw new ValidationException(
                    String.format("Option '%s' was provided, but is empty", HADOOP_CONF_DIR.key()));
        }

        final String hiveVersion = configuration.getString(HIVE_VERSION);
        if (hiveVersion != null && hiveVersion.isEmpty()) {
            throw new ValidationException(
                    String.format("Option '%s' was provided, but is empty", HIVE_VERSION.key()));
        }
    }
}
