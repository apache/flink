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

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_LOCATION;

/** ALTER TABLE DDL to change a Hive table/partition's location. */
public class SqlAlterHiveTableLocation extends SqlAlterHiveTable {

    private final SqlCharStringLiteral location;

    public SqlAlterHiveTableLocation(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList partitionSpec,
            SqlCharStringLiteral location) {
        super(CHANGE_LOCATION, pos, tableName, partitionSpec, createPropList(location));
        this.location = location;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("SET LOCATION");
        location.unparse(writer, leftPrec, rightPrec);
    }

    private static SqlNodeList createPropList(SqlCharStringLiteral location) {
        SqlNodeList res = new SqlNodeList(location.getParserPosition());
        res.add(
                HiveDDLUtils.toTableOption(
                        SqlCreateHiveTable.TABLE_LOCATION_URI,
                        location,
                        location.getParserPosition()));
        return res;
    }
}
