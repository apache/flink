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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.Factory;

/** A collection of {@link ConfigOption} which are consistently used in multiple catalogs. */
@Internal
public class CommonCatalogOptions {

    /**
     * Key used for specifying a default database {@link ConfigOption}.
     *
     * <p>Note that we cannot expose an actual instance of {@link ConfigOption} here as the default
     * values differ between catalogs.
     */
    public static final String DEFAULT_DATABASE_KEY = "default-database";

    /**
     * {@link ConfigOption} which is used during catalog discovery to match it against {@link
     * Factory#factoryIdentifier()}.
     */
    public static final ConfigOption<String> CATALOG_TYPE =
            ConfigOptions.key("type").stringType().noDefaultValue();

    public static final String DEFAULT_CATALOG_STORE_KIND = "generic_in_memory";
    public static final ConfigOption<String> TABLE_CATALOG_STORE_KIND =
            ConfigOptions.key("table.catalog-store.kind")
                    .stringType()
                    .defaultValue(DEFAULT_CATALOG_STORE_KIND)
                    .withDescription(
                            "The kind of catalog store to be used. Out of the box, 'generic_in_memory' and 'file' options are supported.");

    /** Used to filter the specific options for catalog store. */
    public static final String TABLE_CATALOG_STORE_OPTION_PREFIX = "table.catalog-store.";
}
