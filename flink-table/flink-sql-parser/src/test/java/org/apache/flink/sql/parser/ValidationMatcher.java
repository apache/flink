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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlNode;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Matcher that invokes the #validate() of the {@link ExtendedSqlNode} instance. * */
class ValidationMatcher extends BaseMatcher<SqlNode> {
    private String expectedColumnSql;
    private String failMsg;
    private boolean ok;

    public ValidationMatcher expectColumnSql(String s) {
        this.expectedColumnSql = s;
        return this;
    }

    public ValidationMatcher fails(String failMsg) {
        this.failMsg = failMsg;
        this.ok = false;
        return this;
    }

    public ValidationMatcher ok() {
        this.failMsg = null;
        this.ok = true;
        return this;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("test");
    }

    @Override
    public boolean matches(Object item) {
        if (item instanceof ExtendedSqlNode) {
            ExtendedSqlNode createTable = (ExtendedSqlNode) item;

            if (ok) {
                try {
                    createTable.validate();
                } catch (SqlValidateException e) {
                    fail("unexpected exception", e);
                }
            } else if (failMsg != null) {
                try {
                    createTable.validate();
                    fail("expected exception");
                } catch (SqlValidateException e) {
                    assertThat(e).hasMessage(failMsg);
                }
            }

            if (expectedColumnSql != null && item instanceof SqlCreateTable) {
                assertThat(((SqlCreateTable) createTable).getColumnSqlString())
                        .isEqualTo(expectedColumnSql);
            }
            return true;
        } else {
            return false;
        }
    }
}
