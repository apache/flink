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

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Represents the unresolved metadata of a view in a {@link Catalog}.
 *
 * <p>It contains all characteristics that can be expressed in a SQL {@code CREATE VIEW} statement.
 * The framework will resolve instances of this interface to a {@link ResolvedCatalogView} before
 * usage.
 *
 * <p>A catalog implementer can either use {@link #of(Schema, String, String, String, Map)} for a
 * basic implementation of this interface or create a custom class that allows passing
 * catalog-specific objects (if necessary).
 */
@PublicEvolving
public interface CatalogView extends CatalogBaseTable {

    /**
     * Creates a basic implementation of this interface.
     *
     * <p>The signature is similar to a SQL {@code CREATE VIEW} statement.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param originalQuery original text of the view definition
     * @param expandedQuery expanded text of the original view definition with materialized
     *     identifiers
     * @param options options to configure the connector
     */
    static CatalogView of(
            Schema schema,
            @Nullable String comment,
            String originalQuery,
            String expandedQuery,
            Map<String, String> options) {
        return new DefaultCatalogView(schema, comment, originalQuery, expandedQuery, options);
    }

    @Override
    default TableKind getTableKind() {
        return TableKind.VIEW;
    }

    /**
     * Original text of the view definition that also preserves the original formatting.
     *
     * @return the original string literal provided by the user.
     */
    String getOriginalQuery();

    /**
     * Expanded text of the original view definition This is needed because the context such as
     * current DB is lost after the session, in which view is defined, is gone. Expanded query text
     * takes care of this, as an example.
     *
     * <p>For example, for a view that is defined in the context of "default" database with a query
     * {@code select * from test1}, the expanded query text might become {@code select
     * `test1`.`name`, `test1`.`value` from `default`.`test1`}, where table test1 resides in
     * database "default" and has two columns ("name" and "value").
     *
     * @return the view definition in expanded text.
     */
    String getExpandedQuery();
}
