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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

/** Operation to describe a ALTER VIEW .. RENAME to .. statement. */
@Internal
public class AlterViewRenameOperation extends AlterViewOperation {

    private final ObjectIdentifier newViewIdentifier;

    public AlterViewRenameOperation(
            ObjectIdentifier viewIdentifier, ObjectIdentifier newViewIdentifier) {
        super(viewIdentifier);
        this.newViewIdentifier = newViewIdentifier;
    }

    public ObjectIdentifier getNewViewIdentifier() {
        return newViewIdentifier;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER VIEW %s RENAME TO %s",
                viewIdentifier.asSummaryString(), newViewIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        Catalog catalog =
                ctx.getCatalogManager()
                        .getCatalogOrThrowException(getViewIdentifier().getCatalogName());
        try {
            catalog.renameTable(
                    getViewIdentifier().toObjectPath(),
                    getNewViewIdentifier().getObjectName(),
                    false);
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (TableAlreadyExistException | TableNotExistException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
