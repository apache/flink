/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.ObjectIdentifier;

import javax.annotation.Nullable;

import java.util.Optional;

/** Operation of "ALTER TABLE ADD [CONSTRAINT constraintName] ..." clause. * */
public class AlterTableDropConstraintOperation extends AlterTableOperation {

    private final boolean isPrimaryKey;
    private final @Nullable String constraintName;

    public AlterTableDropConstraintOperation(
            ObjectIdentifier tableIdentifier,
            boolean isPrimaryKey,
            @Nullable String constraintName) {
        super(tableIdentifier);
        this.isPrimaryKey = isPrimaryKey;
        this.constraintName = constraintName;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public Optional<String> getConstraintName() {
        return Optional.ofNullable(constraintName);
    }

    @Override
    public String asSummaryString() {
        if (isPrimaryKey) {
            return String.format(
                    "ALTER TABLE %s DROP PRIMARY KEY", tableIdentifier.asSummaryString());
        } else {
            return String.format(
                    "ALTER TABLE %s DROP CONSTRAINT %s",
                    tableIdentifier.asSummaryString(), constraintName);
        }
    }
}
