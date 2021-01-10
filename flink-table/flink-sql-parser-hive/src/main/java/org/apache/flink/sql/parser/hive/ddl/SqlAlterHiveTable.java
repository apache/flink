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

import org.apache.flink.sql.parser.ddl.SqlAlterTableProperties;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Abstract class for ALTER DDL of a Hive table. Any ALTER TABLE operations that need to be encoded
 * as table properties should extend this class.
 */
public abstract class SqlAlterHiveTable extends SqlAlterTableProperties {

    public static final String ALTER_TABLE_OP = "alter.table.op";
    public static final String ALTER_COL_CASCADE = "alter.column.cascade";

    public SqlAlterHiveTable(
            AlterTableOp op,
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList partSpec,
            SqlNodeList propertyList) {
        super(pos, tableName, partSpec, propertyList);
        propertyList.add(HiveDDLUtils.toTableOption(ALTER_TABLE_OP, op.name(), pos));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER TABLE");
        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        SqlNodeList partitionSpec = getPartitionSpec();
        if (partitionSpec != null && partitionSpec.size() > 0) {
            writer.keyword("PARTITION");
            partitionSpec.unparse(
                    writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
        }
    }

    /** Type of ALTER TABLE operation. */
    public enum AlterTableOp {
        CHANGE_TBL_PROPS,
        CHANGE_SERDE_PROPS,
        CHANGE_FILE_FORMAT,
        CHANGE_LOCATION,
        ALTER_COLUMNS
    }
}
