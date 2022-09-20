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

package org.apache.flink.sql.parser.dql;

import org.apache.flink.sql.parser.SqlPartitionSpecProperty;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * DESCRIBE [ EXTENDED] [[catalogName.] dataBasesName].tableNameIdentifier
 * [PARTITION(partitionSpec)] [columnName] sql call. Here we add Rich in className to distinguish
 * from calcite's original SqlDescribeTable.
 */
public class SqlRichDescribeTable extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("DESCRIBE TABLE", SqlKind.DESCRIBE_TABLE);
    protected final SqlIdentifier tableNameIdentifier;
    private final boolean isExtended;
    protected SqlNodeList partitions;
    protected final SqlIdentifier columnName;

    public SqlRichDescribeTable(
            SqlParserPos pos,
            SqlIdentifier tableNameIdentifier,
            boolean isExtended,
            @Nullable SqlNodeList partitions,
            @Nullable SqlIdentifier columnName) {
        super(pos);
        this.tableNameIdentifier = tableNameIdentifier;
        this.isExtended = isExtended;
        this.partitions = partitions;
        this.columnName = columnName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(tableNameIdentifier);
    }

    public boolean isExtended() {
        return isExtended;
    }

    public String[] fullTableName() {
        return tableNameIdentifier.names.toArray(new String[0]);
    }

    public String getColumnName() {
        if (columnName != null) {
            return columnName.getSimple();
        } else {
            return null;
        }
    }

    /**
     * Get partition spec as key-value strings, if only partition key is given, the corresponding
     * value is null.
     */
    public LinkedHashMap<String, String> getPartitions() {
        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
        if (partitions == null) {
            return ret;
        }
        for (SqlNode node : partitions.getList()) {
            SqlPartitionSpecProperty property = (SqlPartitionSpecProperty) node;
            final String value;
            if (property.getValue() == null) {
                value = null;
            } else {
                Comparable<?> comparable = SqlLiteral.value(property.getValue());
                value =
                        comparable instanceof NlsString
                                ? ((NlsString) comparable).getValue()
                                : comparable.toString();
            }

            ret.put(property.getKey().getSimple(), value);
        }
        return ret;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESCRIBE");
        if (isExtended) {
            writer.keyword("EXTENDED");
        }
        tableNameIdentifier.unparse(writer, leftPrec, rightPrec);

        if (partitions != null && partitions.size() > 0) {
            writer.keyword("PARTITION");
            partitions.unparse(writer, leftPrec, rightPrec);
        }

        if (columnName != null) {
            columnName.unparse(writer, leftPrec, rightPrec);
        }
    }
}
