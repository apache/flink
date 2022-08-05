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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Objects;

/** Information of the {@code Table}. */
@PublicEvolving
public class TableInfo {
    private final boolean isTemporary;
    private final ObjectIdentifier identifier;
    private final TableKind tableKind;

    public TableInfo(boolean isTemporary, ObjectIdentifier identifier, TableKind tableKind) {
        this.isTemporary = isTemporary;
        this.identifier = identifier;
        this.tableKind = tableKind;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    public TableKind getTableKind() {
        return tableKind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableInfo)) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        return isTemporary == that.isTemporary
                && identifier.equals(that.identifier)
                && tableKind == that.tableKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTemporary, identifier, tableKind);
    }
}
