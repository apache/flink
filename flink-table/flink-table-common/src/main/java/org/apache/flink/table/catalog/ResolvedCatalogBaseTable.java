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

import org.apache.flink.table.api.TableSchema;

/**
 * A common parent that describes the <i>resolved</i> metadata of a table or view in a catalog.
 *
 * @param <T> {@link CatalogTable} or {@link CatalogView}
 */
public interface ResolvedCatalogBaseTable<T extends CatalogBaseTable> extends CatalogBaseTable {

    /**
     * Returns the original, unresolved metadata object from the {@link Catalog}.
     *
     * <p>This method might be useful if catalog-specific object instances should be directly
     * forwarded from the catalog to a factory.
     */
    T getOrigin();

    /**
     * Returns a fully resolved and validated {@link ResolvedSchema}.
     *
     * <p>Connectors can configure themselves by accessing {@link ResolvedSchema#getPrimaryKey()}
     * and {@link ResolvedSchema#toPhysicalRowDataType()}.
     */
    ResolvedSchema getResolvedSchema();

    /**
     * @deprecated This method returns the deprecated {@link TableSchema} class. The old class was a
     *     hybrid of resolved and unresolved schema information. It has been replaced by the new
     *     {@link ResolvedSchema} which is resolved by the framework and accessible via {@link
     *     #getResolvedSchema()}.
     */
    @Deprecated
    default TableSchema getSchema() {
        return TableSchema.fromResolvedSchema(getResolvedSchema());
    }
}
