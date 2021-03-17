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

package org.apache.flink.sql.parser.hive.dml;

import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.hive.ddl.HiveDDLUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashMap;
import java.util.Map;

/** INSERT statement for a Hive table. */
public class RichSqlHiveInsert extends RichSqlInsert {

    private final SqlNodeList allPartKeys;
    private final Map<SqlIdentifier, SqlProperty> partKeyToSpec;

    public RichSqlHiveInsert(
            SqlParserPos pos,
            SqlNodeList keywords,
            SqlNodeList extendedKeywords,
            SqlNode targetTable,
            SqlNode source,
            SqlNodeList columnList,
            SqlNodeList staticPartitions,
            SqlNodeList allPartKeys) {
        super(pos, keywords, extendedKeywords, targetTable, source, columnList, staticPartitions);
        HiveDDLUtils.unescapePartitionSpec(staticPartitions);
        this.allPartKeys = allPartKeys;
        partKeyToSpec = getPartKeyToSpec(staticPartitions);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        String insertKeyword = "INSERT INTO";
        if (isUpsert()) {
            insertKeyword = "UPSERT INTO";
        } else if (isOverwrite()) {
            insertKeyword = "INSERT OVERWRITE";
        }
        writer.sep(insertKeyword);
        final int opLeft = getOperator().getLeftPrec();
        final int opRight = getOperator().getRightPrec();
        getTargetTable().unparse(writer, opLeft, opRight);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        if (allPartKeys != null && allPartKeys.size() > 0) {
            writer.keyword("PARTITION");
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode node : allPartKeys) {
                writer.sep(",", false);
                SqlIdentifier partKey = (SqlIdentifier) node;
                SqlProperty spec = partKeyToSpec.get(partKey);
                if (spec != null) {
                    spec.unparse(writer, leftPrec, rightPrec);
                } else {
                    partKey.unparse(writer, leftPrec, rightPrec);
                }
            }
            writer.endList(frame);
            writer.newlineAndIndent();
        }
        getSource().unparse(writer, 0, 0);
    }

    private static Map<SqlIdentifier, SqlProperty> getPartKeyToSpec(SqlNodeList staticSpec) {
        Map<SqlIdentifier, SqlProperty> res = new HashMap<>();
        if (staticSpec != null) {
            for (SqlNode node : staticSpec) {
                SqlProperty spec = (SqlProperty) node;
                res.put(spec.getKey(), spec);
            }
        }
        return res;
    }
}
