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

package org.apache.flink.table.catalog.config;

/** Config for catalog and catalog meta-objects. */
public class CatalogConfig {

    /** Flag to distinguish if a meta-object is generic Flink object or not. */
    public static final String IS_GENERIC = "is_generic";

    // Globally reserved prefix for catalog properties.
    // User defined properties should not with this prefix.
    // Used to distinguish properties created by Hive and Flink,
    // as Hive metastore has its own properties created upon table creation and migration between
    // different versions of metastore.
    public static final String FLINK_PROPERTY_PREFIX = "flink.";
}
