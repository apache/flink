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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;

/**
 * Identifies an object in a catalog. It allows to identify objects such as tables, views, function,
 * or types in a catalog. An identifier must be fully qualified. It is the responsibility of the
 * catalog manager to resolve an identifier to an object.
 *
 * <p>While {@link ObjectPath} is used within the same catalog, instances of this class can be used
 * across catalogs.
 *
 * <p>Two objects are considered equal if they share the same object identifier in a stable session
 * context.
 */
public final class ObjectIdentifier implements Serializable {

    private final String catalogName;

    private final String databaseName;

    private final String objectName;

    public static ObjectIdentifier of(String catalogName, String databaseName, String objectName) {
        return new ObjectIdentifier(catalogName, databaseName, objectName);
    }

    private ObjectIdentifier(String catalogName, String databaseName, String objectName) {
        this.catalogName =
                Preconditions.checkNotNull(catalogName, "Catalog name must not be null.");
        this.databaseName =
                Preconditions.checkNotNull(databaseName, "Database name must not be null.");
        this.objectName = Preconditions.checkNotNull(objectName, "Object name must not be null.");
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getObjectName() {
        return objectName;
    }

    public ObjectPath toObjectPath() {
        return new ObjectPath(databaseName, objectName);
    }

    /** List of the component names of this object identifier. */
    public List<String> toList() {
        return Arrays.asList(getCatalogName(), getDatabaseName(), getObjectName());
    }

    /**
     * Returns a string that fully serializes this instance. The serialized string can be used for
     * transmitting or persisting an object identifier.
     */
    public String asSerializableString() {
        return String.format(
                "%s.%s.%s",
                escapeIdentifier(catalogName),
                escapeIdentifier(databaseName),
                escapeIdentifier(objectName));
    }

    /** Returns a string that summarizes this instance for printing to a console or log. */
    public String asSummaryString() {
        return String.join(".", catalogName, databaseName, objectName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectIdentifier that = (ObjectIdentifier) o;
        return catalogName.equals(that.catalogName)
                && databaseName.equals(that.databaseName)
                && objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, objectName);
    }

    @Override
    public String toString() {
        return asSerializableString();
    }
}
