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

package org.apache.flink.table.catalog.exceptions;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;

import javax.annotation.Nullable;

/** Shared by the exceptions of catalogs. */
final class CatalogExceptionUtils {

    /**
     * Returns the identifier of an object in a catalog. Catalogs that don't know their name pass
     * null, and the exception must still be created, so the catalog name is then {@code "null"}.
     */
    static ObjectIdentifier toObjectIdentifier(@Nullable String catalogName, ObjectPath path) {
        return ObjectIdentifier.of(
                String.valueOf(catalogName), path.getDatabaseName(), path.getObjectName());
    }

    private CatalogExceptionUtils() {}
}
