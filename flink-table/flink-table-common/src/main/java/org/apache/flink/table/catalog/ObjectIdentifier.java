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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
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
@PublicEvolving
public final class ObjectIdentifier implements Serializable {

    static final String UNKNOWN = "<UNKNOWN>";

    private final @Nullable String catalogName;
    private final @Nullable String databaseName;
    private final String objectName;

    public static ObjectIdentifier of(String catalogName, String databaseName, String objectName) {
        if (Objects.equals(catalogName, UNKNOWN) || Objects.equals(databaseName, UNKNOWN)) {
            throw new IllegalArgumentException(
                    String.format("Catalog or database cannot be named '%s'", UNKNOWN));
        }
        return new ObjectIdentifier(
                Preconditions.checkNotNull(catalogName, "Catalog name must not be null."),
                Preconditions.checkNotNull(databaseName, "Database name must not be null."),
                Preconditions.checkNotNull(objectName, "Object name must not be null."));
    }

    /**
     * This method allows to create an {@link ObjectIdentifier} without catalog and database name,
     * in order to propagate anonymous objects with unique identifiers throughout the stack.
     *
     * <p>This method for no reason should be exposed to users, as this should be used only when
     * creating anonymous tables with uniquely generated identifiers.
     */
    static ObjectIdentifier ofAnonymous(String objectName) {
        return new ObjectIdentifier(
                null,
                null,
                Preconditions.checkNotNull(objectName, "Object name must not be null."));
    }

    private ObjectIdentifier(
            @Nullable String catalogName, @Nullable String databaseName, String objectName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.objectName = objectName;
    }

    public String getCatalogName() {
        if (catalogName == null) {
            return UNKNOWN;
        }
        return catalogName;
    }

    public String getDatabaseName() {
        if (catalogName == null) {
            return UNKNOWN;
        }
        return databaseName;
    }

    public String getObjectName() {
        return objectName;
    }

    /**
     * Convert this {@link ObjectIdentifier} to {@link ObjectPath}.
     *
     * @throws TableException if the identifier cannot be converted
     */
    public ObjectPath toObjectPath() throws TableException {
        if (catalogName == null) {
            throw new TableException(
                    "This ObjectIdentifier instance refers to an anonymous object, "
                            + "hence it cannot be converted to ObjectPath and cannot be serialized.");
        }
        return new ObjectPath(databaseName, objectName);
    }

    /** List of the component names of this object identifier. */
    public List<String> toList() {
        if (catalogName == null) {
            return Collections.singletonList(getObjectName());
        }
        return Arrays.asList(getCatalogName(), getDatabaseName(), getObjectName());
    }

    /**
     * Returns a string that fully serializes this instance. The serialized string can be used for
     * transmitting or persisting an object identifier.
     *
     * @throws TableException if the identifier cannot be serialized
     */
    public String asSerializableString() throws TableException {
        if (catalogName == null) {
            throw new TableException(
                    "This ObjectIdentifier instance refers to an anonymous object, "
                            + "hence it cannot be converted to ObjectPath and cannot be serialized.");
        }
        return String.format(
                "%s.%s.%s",
                escapeIdentifier(catalogName),
                escapeIdentifier(databaseName),
                escapeIdentifier(objectName));
    }

    /** Returns a string that summarizes this instance for printing to a console or log. */
    public String asSummaryString() {
        if (catalogName == null) {
            return objectName;
        }
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
        return Objects.equals(catalogName, that.catalogName)
                && Objects.equals(databaseName, that.databaseName)
                && objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, objectName);
    }

    @Override
    public String toString() {
        if (catalogName == null) {
            return objectName;
        }
        return asSerializableString();
    }
}
