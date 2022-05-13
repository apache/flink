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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.flink.sql.parser.ddl.SqlTableOption;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase.DATABASE_LOCATION_URI;

/** ALTER Database location DDL for Hive dialect. */
public class SqlAlterHiveDatabaseLocation extends SqlAlterHiveDatabase {

    private final SqlCharStringLiteral location;

    public SqlAlterHiveDatabaseLocation(
            SqlParserPos pos, SqlIdentifier databaseName, SqlCharStringLiteral location) {
        super(pos, databaseName, new SqlNodeList(pos));
        getPropertyList()
                .add(
                        new SqlTableOption(
                                SqlLiteral.createCharString(
                                        DATABASE_LOCATION_URI, location.getParserPosition()),
                                location,
                                location.getParserPosition()));
        this.location = location;
    }

    @Override
    protected AlterHiveDatabaseOp getAlterOp() {
        return AlterHiveDatabaseOp.CHANGE_LOCATION;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("LOCATION");
        location.unparse(writer, leftPrec, rightPrec);
    }
}
