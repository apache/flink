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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Util to validate {@link SqlTableConstraint}. */
public class SqlConstraintValidator {

    /** Returns the column constraints plus the table constraints. */
    public static List<SqlTableConstraint> getFullConstraints(
            List<SqlTableConstraint> tableConstraints, SqlNodeList columnList) {
        List<SqlTableConstraint> ret = new ArrayList<>();
        columnList.forEach(
                column -> {
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    if (tableColumn instanceof SqlTableColumn.SqlRegularColumn) {
                        SqlTableColumn.SqlRegularColumn regularColumn =
                                (SqlTableColumn.SqlRegularColumn) tableColumn;
                        regularColumn.getConstraint().map(ret::add);
                    }
                });
        ret.addAll(tableConstraints);
        return ret;
    }

    /**
     * Check constraints and change the nullability of primary key columns.
     *
     * @throws SqlValidateException if encountered duplicate primary key constraints, or the
     *     constraint is enforced or unique.
     */
    public static void validateAndChangeColumnNullability(
            List<SqlTableConstraint> tableConstraints, SqlNodeList columnList)
            throws SqlValidateException {
        List<SqlTableConstraint> fullConstraints = getFullConstraints(tableConstraints, columnList);
        if (fullConstraints.stream().filter(SqlTableConstraint::isPrimaryKey).count() > 1) {
            throw new SqlValidateException(
                    fullConstraints.get(1).getParserPosition(), "Duplicate primary key definition");
        }
        for (SqlTableConstraint constraint : fullConstraints) {
            validate(constraint);
            Set<String> primaryKeyColumns =
                    Arrays.stream(constraint.getColumnNames()).collect(Collectors.toSet());

            // rewrite primary key's nullability to false
            // e.g. CREATE TABLE tbl (`a` STRING PRIMARY KEY NOT ENFORCED, ...) or
            // CREATE TABLE tbl (`a` STRING, PRIMARY KEY(`a`) NOT ENFORCED) will change `a`
            // to STRING NOT NULL
            for (SqlNode column : columnList) {
                SqlTableColumn tableColumn = (SqlTableColumn) column;
                if (tableColumn instanceof SqlTableColumn.SqlRegularColumn
                        && primaryKeyColumns.contains(tableColumn.getName().getSimple())) {
                    SqlTableColumn.SqlRegularColumn regularColumn =
                            (SqlTableColumn.SqlRegularColumn) column;
                    SqlDataTypeSpec notNullType = regularColumn.getType().withNullable(false);
                    regularColumn.setType(notNullType);
                }
            }
        }
    }

    /** Check table constraint. */
    private static void validate(SqlTableConstraint constraint) throws SqlValidateException {
        if (constraint.isUnique()) {
            throw new SqlValidateException(
                    constraint.getParserPosition(), "UNIQUE constraint is not supported yet");
        }
        if (constraint.isEnforced()) {
            throw new SqlValidateException(
                    constraint.getParserPosition(),
                    "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED "
                            + "controls if the constraint checks are performed on the incoming/outgoing data. "
                            + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode");
        }
    }
}
