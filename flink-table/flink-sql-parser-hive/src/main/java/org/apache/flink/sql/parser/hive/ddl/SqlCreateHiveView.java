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

import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * CREATE View DDL for Hive dialect.
 *
 * <p>CREATE VIEW [IF NOT EXISTS] [db_name.]view_name [(column_name [COMMENT column_comment], ...) ]
 * [COMMENT view_comment] [TBLPROPERTIES (property_name = property_value, ...)] AS SELECT ...;
 */
public class SqlCreateHiveView extends SqlCreateView {

    private SqlNodeList originPropList;

    public SqlCreateHiveView(
            SqlParserPos pos,
            SqlIdentifier viewName,
            SqlNodeList fieldList,
            SqlNode query,
            boolean ifNotExists,
            SqlCharStringLiteral comment,
            SqlNodeList properties) {
        super(
                pos,
                viewName,
                fieldList,
                query,
                false,
                false,
                ifNotExists,
                HiveDDLUtils.unescapeStringLiteral(comment),
                properties);
        HiveDDLUtils.unescapeProperties(properties);
        originPropList = new SqlNodeList(properties.getList(), properties.getParserPosition());
        // mark it as a hive view
        properties.add(
                HiveDDLUtils.toTableOption(
                        FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER, pos));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE VIEW");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        getViewName().unparse(writer, leftPrec, rightPrec);
        if (getFieldList().size() > 0) {
            getFieldList().unparse(writer, 1, rightPrec);
        }
        getComment()
                .ifPresent(
                        c -> {
                            writer.newlineAndIndent();
                            writer.keyword("COMMENT");
                            c.unparse(writer, leftPrec, rightPrec);
                        });
        if (originPropList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("TBLPROPERTIES");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : originPropList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        getQuery().unparse(writer, leftPrec, rightPrec);
    }
}
