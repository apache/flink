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

package org.apache.flink.table.catalog.listener;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

/** When a table is altered, a {@link AlterTableEvent} event will be created and fired. */
@PublicEvolving
public interface AlterTableEvent extends TableModificationEvent {
    CatalogBaseTable newTable();

    boolean ignoreIfNotExists();

    static AlterTableEvent createEvent(
            final CatalogContext context,
            final ObjectIdentifier identifier,
            final CatalogBaseTable newTable,
            final boolean ignoreIfNotExists) {
        return new AlterTableEvent() {
            @Override
            public CatalogBaseTable newTable() {
                return newTable;
            }

            @Override
            public boolean ignoreIfNotExists() {
                return ignoreIfNotExists;
            }

            @Override
            public ObjectIdentifier identifier() {
                return identifier;
            }

            @Override
            public CatalogBaseTable table() {
                throw new IllegalStateException(
                        "There is no table in AlterTableEvent, use identifier() instead.");
            }

            @Override
            public boolean isTemporary() {
                return false;
            }

            @Override
            public CatalogContext context() {
                return context;
            }
        };
    }
}
