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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Base class providing shared parser test utilities for Flink SQL parser tests. */
@Execution(CONCURRENT)
abstract class FlinkSqlParserTestBase {

    public SqlParserFixture fixture() {
        return SqlParserFixture.DEFAULT.withConfig(
                c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }

    protected SqlParserFixture sql(String sql) {
        return fixture().sql(sql);
    }

    protected SqlParserFixture expr(String sql) {
        return sql(sql).expression(true);
    }

    public static BaseMatcher<SqlNode> validated(String validatedSql) {
        return new TypeSafeDiagnosingMatcher<SqlNode>() {
            @Override
            protected boolean matchesSafely(SqlNode item, Description mismatchDescription) {
                if (item instanceof ExtendedSqlNode) {
                    try {
                        ((ExtendedSqlNode) item).validate();
                    } catch (SqlValidateException e) {
                        mismatchDescription.appendText(
                                "Could not validate the node. Exception: \n");
                        mismatchDescription.appendValue(e);
                    }

                    String actual = item.toSqlString(null, true).getSql();
                    return actual.equals(validatedSql);
                }
                mismatchDescription.appendText(
                        "This matcher can be applied only to ExtendedSqlNode.");
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                        "The validated node string representation should be equal to: \n");
                description.appendText(validatedSql);
            }
        };
    }
}
