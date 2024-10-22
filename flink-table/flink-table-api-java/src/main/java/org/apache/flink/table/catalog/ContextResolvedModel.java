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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * This class contains information about a model and its relationship with a {@link Catalog}, if
 * any.
 *
 * <p>There can be 2 kinds of {@link ContextResolvedModel}:
 *
 * <ul>
 *   <li>A permanent model: a model which is stored in a {@link Catalog} and has an associated
 *       unique {@link ObjectIdentifier}.
 *   <li>A temporary model: a model which is stored in the {@link CatalogManager}, has an associated
 *       unique {@link ObjectIdentifier} and is flagged as temporary.
 * </ul>
 *
 * <p>The different handling of temporary and permanent model is {@link Catalog} and {@link
 * CatalogManager} instance specific, hence for these two kind of models, an instance of this object
 * represents the relationship between the specific {@link ResolvedCatalogModel} instance and the
 * specific {@link Catalog}/{@link CatalogManager} instances. For example, the same {@link
 * ResolvedCatalogModel} can be temporary for one catalog, but permanent for another one.
 */
@Internal
public final class ContextResolvedModel {

    private final ObjectIdentifier objectIdentifier;
    private final @Nullable Catalog catalog;
    private final ResolvedCatalogModel resolvedModel;

    public static ContextResolvedModel permanent(
            ObjectIdentifier identifier, Catalog catalog, ResolvedCatalogModel resolvedModel) {
        return new ContextResolvedModel(
                identifier, Preconditions.checkNotNull(catalog), resolvedModel);
    }

    public static ContextResolvedModel temporary(
            ObjectIdentifier identifier, ResolvedCatalogModel resolvedModel) {
        return new ContextResolvedModel(identifier, null, resolvedModel);
    }

    private ContextResolvedModel(
            ObjectIdentifier objectIdentifier,
            @Nullable Catalog catalog,
            ResolvedCatalogModel resolvedModel) {
        this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier);
        this.catalog = catalog;
        this.resolvedModel = Preconditions.checkNotNull(resolvedModel);
    }

    /** @return true if the model is temporary. */
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
    public ResolvedCatalogModel getResolvedModel() {
        return resolvedModel;
    }

    /** Returns the original metadata object returned by the catalog. */
    public CatalogModel getModel() {
        return resolvedModel.getOrigin();
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
        ContextResolvedModel that = (ContextResolvedModel) o;
        return Objects.equals(objectIdentifier, that.objectIdentifier)
                && Objects.equals(catalog, that.catalog)
                && Objects.equals(resolvedModel, that.resolvedModel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectIdentifier, catalog, resolvedModel);
    }
}
