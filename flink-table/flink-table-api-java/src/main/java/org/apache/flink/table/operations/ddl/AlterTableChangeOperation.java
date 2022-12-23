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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableChange;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Alter table with new table definition and table changes represents the modification. */
public class AlterTableChangeOperation extends AlterTableOperation {

    private final List<TableChange> tableChanges;
    private final CatalogTable newTable;

    public AlterTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogTable newTable) {
        super(tableIdentifier);
        this.tableChanges = Collections.unmodifiableList(tableChanges);
        this.newTable = newTable;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public CatalogTable getNewTable() {
        return newTable;
    }

    @Override
    public String asSummaryString() {
        String changes =
                tableChanges.stream()
                        .map(
                                tableChange -> {
                                    if (tableChange instanceof TableChange.SetOption) {
                                        TableChange.SetOption setChange =
                                                (TableChange.SetOption) tableChange;
                                        return String.format(
                                                "  SET '%s' = '%s'",
                                                setChange.getKey(), setChange.getValue());
                                    } else if (tableChange instanceof TableChange.ResetOption) {
                                        TableChange.ResetOption resetChange =
                                                (TableChange.ResetOption) tableChange;
                                        return String.format("  RESET '%s'", resetChange.getKey());
                                    } else {
                                        throw new UnsupportedOperationException(
                                                String.format(
                                                        "Unknown table change: %s.", tableChange));
                                    }
                                })
                        .collect(Collectors.joining(",\n"));
        return String.format("ALTER TABLE %s\n%s", tableIdentifier.asSummaryString(), changes);
    }
}
