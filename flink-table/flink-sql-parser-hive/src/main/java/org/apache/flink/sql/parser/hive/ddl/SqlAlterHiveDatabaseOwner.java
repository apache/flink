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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** ALTER Database owner DDL for Hive dialect. */
public class SqlAlterHiveDatabaseOwner extends SqlAlterHiveDatabase {

    public static final String DATABASE_OWNER_NAME = "hive.database.owner.name";
    public static final String DATABASE_OWNER_TYPE = "hive.database.owner.type";
    public static final String USER_OWNER = "user";
    public static final String ROLE_OWNER = "role";

    private final String ownerType;
    private final SqlIdentifier ownerName;

    public SqlAlterHiveDatabaseOwner(
            SqlParserPos pos,
            SqlIdentifier databaseName,
            String ownerType,
            SqlIdentifier ownerName) {
        super(pos, databaseName, new SqlNodeList(pos));
        SqlParserPos ownerPos = ownerName.getParserPosition();
        getPropertyList()
                .add(
                        new SqlTableOption(
                                SqlLiteral.createCharString(DATABASE_OWNER_TYPE, ownerPos),
                                SqlLiteral.createCharString(ownerType, ownerPos),
                                ownerPos));
        getPropertyList()
                .add(
                        new SqlTableOption(
                                SqlLiteral.createCharString(DATABASE_OWNER_NAME, ownerPos),
                                SqlLiteral.createCharString(ownerName.getSimple(), ownerPos),
                                ownerPos));
        this.ownerName = ownerName;
        this.ownerType = ownerType;
    }

    @Override
    protected AlterHiveDatabaseOp getAlterOp() {
        return AlterHiveDatabaseOp.CHANGE_OWNER;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("OWNER");
        if (ownerType.equalsIgnoreCase(USER_OWNER)) {
            writer.keyword("USER");
        } else {
            writer.keyword("ROLE");
        }
        ownerName.unparse(writer, leftPrec, rightPrec);
    }
}
