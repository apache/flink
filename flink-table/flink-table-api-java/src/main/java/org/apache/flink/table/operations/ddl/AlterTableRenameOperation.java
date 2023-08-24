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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

/** Operation to describe a ALTER TABLE .. RENAME to .. statement. */
@Internal
public class AlterTableRenameOperation extends AlterTableOperation {
    private final ObjectIdentifier newTableIdentifier;

    public AlterTableRenameOperation(
            ObjectIdentifier tableIdentifier,
            ObjectIdentifier newTableIdentifier,
            boolean ignoreIfNotExists) {
        super(tableIdentifier, ignoreIfNotExists);
        this.newTableIdentifier = newTableIdentifier;
    }

    public ObjectIdentifier getNewTableIdentifier() {
        return newTableIdentifier;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER TABLE %s%s RENAME TO %s",
                ignoreIfTableNotExists ? "IF EXISTS " : "",
                tableIdentifier.asSummaryString(),
                newTableIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final Catalog catalog =
                ctx.getCatalogManager()
                        .getCatalogOrThrowException(getTableIdentifier().getCatalogName());
        try {
            catalog.renameTable(
                    getTableIdentifier().toObjectPath(),
                    getNewTableIdentifier().getObjectName(),
                    ignoreIfTableNotExists());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (TableAlreadyExistException | TableNotExistException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
