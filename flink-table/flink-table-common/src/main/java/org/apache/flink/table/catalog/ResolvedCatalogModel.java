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

import java.util.Map;

/**
 * A validated {@link CatalogModel} that is backed by the original metadata coming from the {@link
 * Catalog} but resolved by the framework.
 */
@PublicEvolving
public interface ResolvedCatalogModel extends CatalogModel {
    /**
     * Returns the original, unresolved metadata object from the {@link Catalog}.
     *
     * <p>This method might be useful if catalog-specific object instances should be directly
     * forwarded from the catalog to a factory.
     */
    CatalogModel getOrigin();

    /** Returns a fully resolved and validated {@link ResolvedSchema} inputSchema. */
    ResolvedSchema getResolvedInputSchema();

    /** Returns a fully resolved and validated {@link ResolvedSchema} outputSchema. */
    ResolvedSchema getResolvedOutputSchema();

    /**
     * Serializes this instance into a map of string-based properties.
     *
     * <p>Compared to the pure table options in {@link #getOptions()}, the map includes input
     * schema, output schema, comment and options.
     */
    Map<String, String> toProperties();

    /**
     * Creates an instance of {@link CatalogModel} from a map of string properties that were
     * previously created with {@link ResolvedCatalogModel#toProperties()}.
     *
     * @param properties serialized version of a {@link ResolvedCatalogModel} that includes input
     *     schema, output schema, comment and options.
     */
    static CatalogModel fromProperties(Map<String, String> properties) {
        return CatalogPropertiesUtil.deserializeCatalogModel(properties);
    }

    /**
     * Creates a basic implementation of this interface.
     *
     * @param origin origin unresolved catalog model
     * @param resolvedInputSchema resolved input schema
     * @param resolvedOutputSchema resolved output schema
     */
    static ResolvedCatalogModel of(
            CatalogModel origin,
            ResolvedSchema resolvedInputSchema,
            ResolvedSchema resolvedOutputSchema) {
        return new DefaultResolvedCatalogModel(origin, resolvedInputSchema, resolvedOutputSchema);
    }
}
