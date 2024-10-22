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
import org.apache.flink.table.api.Schema;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;

/**
 * A validated {@link CatalogModel} that is backed by the original metadata coming from the {@link
 * Catalog} but resolved by the framework.
 */
@PublicEvolving
public final class DefaultResolvedCatalogModel implements ResolvedCatalogModel, CatalogModel {
    private final CatalogModel origin;

    private final ResolvedSchema resolvedInputSchema;

    private final ResolvedSchema resolvedOutputSchema;

    public DefaultResolvedCatalogModel(
            CatalogModel origin,
            ResolvedSchema resolvedInputSchema,
            ResolvedSchema resolvedOutputSchema) {
        this.origin =
                Preconditions.checkNotNull(origin, "Original catalog model must not be null.");
        this.resolvedInputSchema =
                Preconditions.checkNotNull(
                        resolvedInputSchema, "Resolved input schema must not be null.");
        this.resolvedOutputSchema =
                Preconditions.checkNotNull(
                        resolvedOutputSchema, "Resolved output schema must not be null.");
    }

    @Override
    public CatalogModel getOrigin() {
        return origin;
    }

    @Override
    public ResolvedSchema getResolvedInputSchema() {
        return resolvedInputSchema;
    }

    @Override
    public ResolvedSchema getResolvedOutputSchema() {
        return resolvedOutputSchema;
    }

    @Override
    public Map<String, String> toProperties() {
        return CatalogPropertiesUtil.serializeResolvedCatalogModel(this);
    }

    // --------------------------------------------------------------------------------------------
    // Delegations to original CatalogModel
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, String> getOptions() {
        return origin.getOptions();
    }

    @Override
    public Schema getInputSchema() {
        return origin.getInputSchema();
    }

    @Override
    public Schema getOutputSchema() {
        return origin.getOutputSchema();
    }

    @Override
    public String getComment() {
        return origin.getComment();
    }

    @Override
    public ResolvedCatalogModel copy() {
        return new DefaultResolvedCatalogModel(
                origin.copy(), resolvedInputSchema, resolvedOutputSchema);
    }

    @Override
    public ResolvedCatalogModel copy(Map<String, String> modelOptions) {
        return new DefaultResolvedCatalogModel(
                origin.copy(modelOptions), resolvedInputSchema, resolvedOutputSchema);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultResolvedCatalogModel that = (DefaultResolvedCatalogModel) o;
        return Objects.equals(origin, that.origin)
                && Objects.equals(resolvedInputSchema, that.resolvedInputSchema)
                && Objects.equals(resolvedOutputSchema, that.resolvedOutputSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin, resolvedInputSchema, resolvedOutputSchema);
    }

    @Override
    public String toString() {
        return "ResolvedCatalogModel{"
                + "origin="
                + origin
                + ", resolvedInputSchema="
                + resolvedInputSchema
                + ", resolvedOutputSchema="
                + resolvedOutputSchema
                + '}';
    }
}
