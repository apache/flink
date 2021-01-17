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

package org.apache.flink.table.catalog.hive.descriptors;

import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_HIVE_VERSION;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_TYPE_VALUE_HIVE;

/** Catalog descriptor for {@link HiveCatalog}. */
public class HiveCatalogDescriptor extends CatalogDescriptor {

    private String hiveSitePath;
    private String hiveVersion;

    // TODO : set default database
    public HiveCatalogDescriptor() {
        super(CATALOG_TYPE_VALUE_HIVE, 1);
    }

    public HiveCatalogDescriptor hiveSitePath(String hiveSitePath) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveSitePath));
        this.hiveSitePath = hiveSitePath;

        return this;
    }

    public HiveCatalogDescriptor hiveVersion(String hiveVersion) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveVersion));
        this.hiveVersion = hiveVersion;
        return this;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        if (hiveSitePath != null) {
            properties.putString(CATALOG_HIVE_CONF_DIR, hiveSitePath);
        }

        if (hiveVersion != null) {
            properties.putString(CATALOG_HIVE_VERSION, hiveVersion);
        }

        return properties.asMap();
    }
}
