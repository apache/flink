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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Objects;

/**
 * Information of the table or view.
 *
 * <p>Note: It is not equivalent to the {@link CatalogTable} that also contains {@link Schema}. The
 * invoker can get basic information for the table without to read all information in the remote
 * catalog.
 */
@PublicEvolving
public class TableInfo {
    private final ObjectIdentifier identifier;
    private final TableKind tableKind;

    public TableInfo(ObjectIdentifier identifier, TableKind tableKind) {
        this.identifier = identifier;
        this.tableKind = tableKind;
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
        return identifier.equals(that.identifier) && tableKind == that.tableKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, tableKind);
    }

    @Override
    public String toString() {
        return "TableInfo{" + "identifier=" + identifier + ", tableKind=" + tableKind + '}';
    }
}
