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

package org.apache.flink.table.planner.utils;

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Utils methods for converting sql to operations. */
public class OperationConverterUtils {

    private OperationConverterUtils() {}

    public static TableDistribution getDistributionFromSqlDistribution(
            SqlDistribution distribution) {
        TableDistribution.Kind kind =
                TableDistribution.Kind.valueOf(
                        distribution
                                .getDistributionKind()
                                .orElse(TableDistribution.Kind.UNKNOWN.toString()));
        Integer bucketCount = null;
        SqlNumericLiteral count = distribution.getBucketCount();
        if (count != null && count.isInteger()) {
            bucketCount = ((BigDecimal) (count).getValue()).intValue();
        }

        SqlNodeList columns = distribution.getBucketColumns();
        List<String> bucketColumns =
                SqlParseUtils.extractList(columns, p -> ((SqlIdentifier) p).getSimple());
        return TableDistribution.of(kind, bucketCount, bucketColumns);
    }

    public static String getQuotedSqlString(SqlNode sqlNode, FlinkPlannerImpl flinkPlanner) {
        SqlParser.Config parserConfig = flinkPlanner.config().getParserConfig();
        SqlDialect dialect =
                new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT
                                .withQuotedCasing(parserConfig.unquotedCasing())
                                .withConformance(parserConfig.conformance())
                                .withUnquotedCasing(parserConfig.unquotedCasing())
                                .withIdentifierQuoteString(parserConfig.quoting().string));
        return sqlNode.toSqlString(dialect).getSql();
    }

    public static Set<String> getColumnNames(
            SqlNodeList sqlNodeList,
            Function<SqlIdentifier, String> errMessage,
            String duplicatedErrMsgPrefix) {
        Set<String> distinctNames = new HashSet<>();
        for (SqlNode sqlNode : sqlNodeList) {
            String name =
                    SqlParseUtils.extractSimpleColumnName((SqlIdentifier) sqlNode, errMessage);
            if (!distinctNames.add(name)) {
                throw new ValidationException(
                        String.format("%sDuplicate column `%s`.", duplicatedErrMsgPrefix, name));
            }
        }
        return distinctNames;
    }
}
