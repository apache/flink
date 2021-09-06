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
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Operation of "ALTER TABLE IF EXISTS ADD [CONSTRAINT constraintName] ..." clause.
 *
 * <p>Note: only primary key is supported now.
 */
public class AlterTableAddConstraintOperation extends AlterTableOperation {
    private final String constraintName;
    private final String[] columnNames;

    public AlterTableAddConstraintOperation(
            ObjectIdentifier tableIdentifier,
            @Nullable String constraintName,
            String[] columnNames,
            boolean ifExists) {
        super(tableIdentifier, ifExists);
        this.constraintName = constraintName;
        this.columnNames = columnNames;
    }

    public Optional<String> getConstraintName() {
        return Optional.ofNullable(constraintName);
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", tableIdentifier);
        if (getConstraintName().isPresent()) {
            params.put("constraintName", this.constraintName);
        }
        params.put("columns", this.columnNames);

        return OperationUtils.formatWithChildren(
                String.format(
                        "ALTER TABLE %sADD CONSTRAINT",
                        ifExists ? "IF EXISTS " : StringUtils.EMPTY),
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }
}
