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

/** Represents a view in a catalog. */
public interface CatalogView extends CatalogBaseTable {

    /**
     * Original text of the view definition that also perserves the original formatting.
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
