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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class contains information about a table, its {@link ResolvedSchema}, its options and its
 * relationship with a {@link Catalog}, if any.
 *
 * <p>There can be 3 kinds of {@link ContextResolvedTable}:
 *
 * <ul>
 *   <li>A permanent table: a table which is stored in a {@link Catalog} and has an associated
 *       unique {@link ObjectIdentifier}.
 *   <li>A temporary table: a table which is stored in the {@link CatalogManager}, has an associated
 *       unique {@link ObjectIdentifier} and is flagged as temporary.
 *   <li>An anonymous/inline table: a table which is not stored in a catalog and doesn't have an
 *       associated unique {@link ObjectIdentifier}.
 * </ul>
 *
 * <p>The different handling of temporary and permanent tables is {@link Catalog} and {@link
 * CatalogManager} instance specific, hence for these two kind of tables, an instance of this object
 * represents the relationship between the specific {@link ResolvedCatalogBaseTable} instance and
 * the specific {@link Catalog}/{@link CatalogManager} instances. For example, the same {@link
 * ResolvedCatalogBaseTable} can be temporary for one catalog, but permanent for another one.
 */
@Internal
public final class ContextResolvedTable {

    private static final AtomicInteger uniqueId = new AtomicInteger(0);

    private final ObjectIdentifier objectIdentifier;
    private final @Nullable Catalog catalog;
    private final ResolvedCatalogBaseTable<?> resolvedTable;
    private final boolean anonymous;

    public static ContextResolvedTable permanent(
            ObjectIdentifier identifier,
            Catalog catalog,
            ResolvedCatalogBaseTable<?> resolvedTable) {
        return new ContextResolvedTable(
                identifier, Preconditions.checkNotNull(catalog), resolvedTable, false);
    }

    public static ContextResolvedTable temporary(
            ObjectIdentifier identifier, ResolvedCatalogBaseTable<?> resolvedTable) {
        return new ContextResolvedTable(identifier, null, resolvedTable, false);
    }

    public static ContextResolvedTable anonymous(ResolvedCatalogBaseTable<?> resolvedTable) {
        return new ContextResolvedTable(
                ObjectIdentifier.ofAnonymous(
                        generateAnonymousStringIdentifier(null, resolvedTable)),
                null,
                resolvedTable,
                true);
    }

    public static ContextResolvedTable anonymous(
            String hint, ResolvedCatalogBaseTable<?> resolvedTable) {
        return new ContextResolvedTable(
                ObjectIdentifier.ofAnonymous(
                        generateAnonymousStringIdentifier(hint, resolvedTable)),
                null,
                resolvedTable,
                true);
    }

    private ContextResolvedTable(
            ObjectIdentifier objectIdentifier,
            @Nullable Catalog catalog,
            ResolvedCatalogBaseTable<?> resolvedTable,
            boolean anonymous) {
        this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier);
        this.catalog = catalog;
        this.resolvedTable = Preconditions.checkNotNull(resolvedTable);
        this.anonymous = anonymous;
    }

    public boolean isAnonymous() {
        return this.anonymous;
    }

    /** @return true if the table is temporary. An anonymous table is always temporary. */
    public boolean isTemporary() {
        return catalog == null;
    }

    public boolean isPermanent() {
        return !isTemporary();
    }

    public ObjectIdentifier getIdentifier() {
        return objectIdentifier;
    }

    /** Returns empty if {@link #isPermanent()} is false. */
    public Optional<Catalog> getCatalog() {
        return Optional.ofNullable(catalog);
    }

    /** Returns a fully resolved catalog object. */
    @SuppressWarnings("unchecked")
    public <T extends ResolvedCatalogBaseTable<?>> T getResolvedTable() {
        return (T) resolvedTable;
    }

    public ResolvedSchema getResolvedSchema() {
        return resolvedTable.getResolvedSchema();
    }

    /** Returns the original metadata object returned by the catalog. */
    @SuppressWarnings("unchecked")
    public <T extends CatalogBaseTable> T getTable() {
        return (T) resolvedTable.getOrigin();
    }

    /**
     * Copy the {@link ContextResolvedTable}, replacing the underlying {@link CatalogTable} options.
     */
    public ContextResolvedTable copy(Map<String, String> newOptions) {
        if (resolvedTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            throw new ValidationException(
                    String.format("View '%s' cannot be enriched with new options.", this));
        }
        return new ContextResolvedTable(
                objectIdentifier,
                catalog,
                ((ResolvedCatalogTable) resolvedTable).copy(newOptions),
                false);
    }

    /** Copy the {@link ContextResolvedTable}, replacing the underlying {@link ResolvedSchema}. */
    public ContextResolvedTable copy(ResolvedSchema newSchema) {
        return new ContextResolvedTable(
                objectIdentifier,
                catalog,
                new ResolvedCatalogTable((CatalogTable) resolvedTable.getOrigin(), newSchema),
                false);
    }

    /**
     * This method tries to return the connector name of the table, trying to provide a bit more
     * helpful toString for anonymous tables. It's only to help users to debug, and its return value
     * should not be relied on.
     */
    private static String generateAnonymousStringIdentifier(
            @Nullable String hint, ResolvedCatalogBaseTable<?> resolvedTable) {
        // Planner can do some fancy optimizations' logic squashing two sources together in the same
        // operator. Because this logic is string based, anonymous tables still need some kind of
        // unique string based identifier that can be used later by the planner.
        if (hint == null) {
            try {
                hint = resolvedTable.getOptions().get(FactoryUtil.CONNECTOR.key());
            } catch (Exception ignored) {
            }
        }

        int id = uniqueId.incrementAndGet();
        if (hint == null) {
            return "*anonymous$" + id + "*";
        }

        return "*anonymous_" + hint + "$" + id + "*";
    }

    @Override
    public String toString() {
        return objectIdentifier.asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextResolvedTable that = (ContextResolvedTable) o;
        return anonymous == that.anonymous
                && Objects.equals(objectIdentifier, that.objectIdentifier)
                && Objects.equals(catalog, that.catalog)
                && Objects.equals(resolvedTable, that.resolvedTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectIdentifier, catalog, resolvedTable, anonymous);
    }
}
